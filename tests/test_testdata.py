"""测试数据管理（TDM）单元测试。

覆盖：数据池 CRUD、批量导入（重复跳过 / upsert）、多环境隔离、
借用锁定、并发不重、归还恢复可用、续借、批量归还、强制回收、
过期提醒与宽限期强制回收、标签 / 状态筛选、历史记录与统计。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import NotificationManager, TestDataError, TestDataManager
from storage import StoreRegistry


class TDMTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = StoreRegistry(os.path.join(self.tmp.name, "store"),
                                      shard_size=50)
        self.notify = NotificationManager(self.registry)
        self.tdm = TestDataManager(self.registry, self.tmp.name,
                                   notify_manager=self.notify)
        self.pid = "proj_demo"
        self.pool = self.tdm.create_pool(self.pid, {
            "name": "账号池", "envs": ["dev", "staging"],
            "default_ttl": 3600, "overdue_policy": "remind",
        })
        self.tdm.import_items(self.pool["id"], [
            {"env": "dev", "data_key": f"u{i}", "payload": {"v": i},
             "tags": ["vip" if i % 2 else "normal"]}
            for i in range(1, 6)
        ] + [
            {"env": "staging", "data_key": "u1", "payload": {"v": 1}},
        ])

    def tearDown(self):
        self.tmp.cleanup()

    def _item(self, key, env="dev"):
        items = self.tdm.list_items(pool_id=self.pool["id"], env=env)
        return next(i for i in items if i["data_key"] == key)


class TestPoolAndImport(TDMTestBase):
    def test_pool_requires_name(self):
        with self.assertRaises(TestDataError):
            self.tdm.create_pool(self.pid, {"name": "  "})

    def test_pool_rejects_unknown_env(self):
        with self.assertRaises(TestDataError):
            self.tdm.create_pool(self.pid, {"name": "p", "envs": ["qa"]})

    def test_import_dedup_and_upsert(self):
        r = self.tdm.import_items(self.pool["id"], [
            {"env": "dev", "data_key": "u1", "payload": {"v": 999}},  # 重复
            {"env": "dev", "data_key": "u9", "payload": {}},          # 新增
        ])
        self.assertEqual(r["created"], 1)
        self.assertEqual(r["skipped"], 1)
        # 默认跳过：内容不变
        self.assertEqual(self._item("u1")["payload"], {"v": 1})
        r2 = self.tdm.import_items(self.pool["id"], [
            {"env": "dev", "data_key": "u1", "payload": {"v": 999}},
        ], upsert=True)
        self.assertEqual(r2["updated"], 1)
        self.assertEqual(self._item("u1")["payload"], {"v": 999})

    def test_cannot_remove_env_with_data(self):
        with self.assertRaises(TestDataError):
            self.tdm.update_pool(self.pool["id"], {"envs": ["dev"]})

    def test_list_pools_counts(self):
        pools = self.tdm.list_pools(self.pid)
        p = next(x for x in pools if x["id"] == self.pool["id"])
        self.assertEqual(p["item_count"], 6)
        self.assertEqual(p["by_env"]["dev"]["total"], 5)
        self.assertEqual(p["by_env"]["dev"]["available"], 5)


class TestBorrowReturn(TDMTestBase):
    def test_borrow_locks_item(self):
        item = self._item("u1")
        lease = self.tdm.borrow_item(item["id"], "张三", purpose="回归")
        self.assertEqual(lease["status"], "active")
        self.assertEqual(lease["held_seconds"], 0)

        item2 = self.tdm.get_item(item["id"])
        self.assertEqual(item2["status"], "borrowed")
        self.assertEqual(item2["current_lease"]["borrower"], "张三")

        # 被借出期间，其他人拿不到
        with self.assertRaises(TestDataError):
            self.tdm.borrow_item(item["id"], "李四")

    def test_return_makes_available(self):
        item = self._item("u1")
        lease = self.tdm.borrow_item(item["id"], "张三")
        self.tdm.return_lease(lease["id"])
        self.assertEqual(self.tdm.get_item(item["id"])["status"], "available")
        # 别人马上能借
        lease2 = self.tdm.borrow_item(item["id"], "李四")
        self.assertEqual(lease2["borrower"], "李四")
        # 已结束的单子不能重复归还
        with self.assertRaises(TestDataError):
            self.tdm.return_lease(lease2 and lease["id"])

    def test_only_borrower_can_return(self):
        item = self._item("u1")
        lease = self.tdm.borrow_item(item["id"], "张三")
        with self.assertRaises(TestDataError):
            self.tdm.return_lease(lease["id"], borrower="李四")
        # 强制回收不校验借用人
        self.tdm.force_return(lease["id"])
        self.assertEqual(self.tdm.get_item(item["id"])["status"], "available")

    def test_borrow_by_conditions_and_tags(self):
        r = self.tdm.borrow(self.pool["id"], "bot", "dev", count=2,
                           tags=["vip"])
        self.assertEqual(r["borrowed"], 2)
        for l in r["leases"]:
            self.assertIn("vip", (l.get("tags") or []))
        # vip 数据（u1,u3,u5）还剩 1 条，要 2 条整单失败
        with self.assertRaises(TestDataError):
            self.tdm.borrow(self.pool["id"], "bot2", "dev", count=2,
                            tags=["vip"])
        # partial=True 时有多少借多少，不报错
        r2 = self.tdm.borrow(self.pool["id"], "bot2", "dev", count=5,
                             tags=["vip"], partial=True)
        self.assertEqual(r2["borrowed"], 1)

    def test_env_isolation_same_logical_data(self):
        # 同一份逻辑数据 u1 在 dev / staging 各有一条，互不串用
        dev = self._item("u1", "dev")
        stg = self._item("u1", "staging")
        self.tdm.borrow_item(dev["id"], "张三")
        # dev 锁住不影响 staging
        lease = self.tdm.borrow_item(stg["id"], "李四")
        self.assertEqual(lease["env"], "staging")
        # 不能借池不支持的环境
        with self.assertRaises(TestDataError):
            self.tdm.borrow(self.pool["id"], "x", "prod")

    def test_concurrent_borrow_never_double_allocate(self):
        items = self.tdm.list_items(pool_id=self.pool["id"], env="dev")
        item_id = items[0]["id"]
        winners, losers = [], []

        def race(user):
            try:
                self.tdm.borrow_item(item_id, user)
                winners.append(user)
            except TestDataError:
                losers.append(user)

        threads = [threading.Thread(target=race, args=(f"u{i}",))
                   for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 11)

    def test_renew_extends_and_clears_overdue(self):
        item = self._item("u2")
        lease = self.tdm.borrow_item(item["id"], "张三", ttl=100)
        due0 = lease["due_at"]
        renewed = self.tdm.renew(lease["id"], extra_seconds=1000)
        self.assertGreater(renewed["due_at"], due0)

    def test_return_by_borrower_batch(self):
        for key in ("u1", "u2", "u3"):
            self.tdm.borrow_item(self._item(key)["id"], "张三")
        self.tdm.borrow_item(self._item("u4")["id"], "李四")
        r = self.tdm.return_by_borrower(self.pid, "张三")
        self.assertEqual(r["returned"], 3)
        open_leases = self.tdm.list_leases(self.pid, status="active")
        self.assertEqual(len(open_leases), 1)
        self.assertEqual(open_leases[0]["borrower"], "李四")


class TestOverdue(TDMTestBase):
    def _expired_lease(self, policy="remind", grace=1800):
        pool = self.tdm.create_pool(self.pid, {
            "name": policy + "池", "envs": ["dev"], "default_ttl": 3600,
            "overdue_policy": policy, "grace_seconds": grace})
        self.tdm.import_items(pool["id"], [
            {"env": "dev", "data_key": "x1"},
            {"env": "dev", "data_key": "x2"},
        ])
        item = self.tdm.list_items(pool_id=pool["id"], env="dev")[0]
        lease = self.tdm.borrow_item(item["id"], "王五", ttl=3600)
        # 把借出时间挪到 2 小时前 -> 已过期 1 小时
        past = time.time() - 7200
        self.registry.store("tdm_leases").update(
            lease["id"], {"borrowed_at": past, "due_at": past + 3600})
        return pool, self.tdm.get_lease(lease["id"])

    def test_mark_overdue_and_remind(self):
        _, lease = self._expired_lease()
        r = self.tdm.sweep_overdue()
        self.assertGreaterEqual(r["marked_overdue"], 1)
        after = self.tdm.get_lease(lease["id"])
        self.assertEqual(after["status"], "overdue")
        self.assertEqual(after["remind_count"], 1)
        self.assertGreater(after["overdue_seconds"], 3000)
        # 过期后数据仍锁定，别人不能借
        with self.assertRaises(TestDataError):
            self.tdm.borrow_item(after["item_id"], "赵六")

    def test_remind_throttled_and_idempotent(self):
        _, lease = self._expired_lease()
        self.tdm.sweep_overdue()
        # 立刻再扫：节流，不重复提醒，也不重复计数
        r2 = self.tdm.sweep_overdue()
        self.assertEqual(r2["reminded"], 0)
        self.assertEqual(self.tdm.get_lease(lease["id"])["remind_count"], 1)

    def test_reclaim_after_grace(self):
        _, lease = self._expired_lease(policy="reclaim", grace=1800)
        r = self.tdm.sweep_overdue()
        self.assertGreaterEqual(r["reclaimed"], 1)
        after = self.tdm.get_lease(lease["id"])
        self.assertEqual(after["status"], "reclaimed")
        # 回收后数据恢复可用
        self.assertEqual(self.tdm.get_item(after["item_id"])["status"],
                         "available")

    def test_not_reclaimed_before_grace(self):
        _, lease = self._expired_lease(policy="reclaim", grace=7200)
        r = self.tdm.sweep_overdue()
        self.assertEqual(r["reclaimed"], 0)
        self.assertEqual(self.tdm.get_lease(lease["id"])["status"], "overdue")

    def test_reclaimed_by_notify_integration(self):
        # 在项目上挂一个 webhook，过期提醒时应产生通知事件记录
        self.notify.create(self.pid, {
            "type": "webhook", "name": "h",
            "config": {"url": "https://example.com/x"},
            "events": ["tdm.overdue_reminder"],
        })
        self._expired_lease()
        self.tdm.sweep_overdue()
        events = self.notify.events(self.pid)
        self.assertTrue(any(e["event"] == "tdm.overdue_reminder" for e in events))


class TestQueryHistoryStats(TDMTestBase):
    def test_filters_and_history(self):
        a = self._item("u1")
        lease = self.tdm.borrow_item(a["id"], "张三")
        self.tdm.return_lease(lease["id"])
        self.tdm.borrow_item(a["id"], "李四")

        hist = self.tdm.item_history(a["id"])
        self.assertEqual(len(hist["leases"]), 2)
        self.assertEqual(hist["leases"][0]["borrower"], "李四")  # 倒序

        only_borrowed = self.tdm.list_items(pool_id=self.pool["id"],
                                            status="borrowed")
        self.assertEqual(len(only_borrowed), 1)

        tag_items = self.tdm.list_items(pool_id=self.pool["id"],
                                        env="dev", tag="vip")
        self.assertEqual({i["data_key"] for i in tag_items},
                         {"u1", "u3", "u5"})

    def test_delete_pool_blocked_when_borrowed(self):
        self.tdm.borrow_item(self._item("u1")["id"], "张三")
        with self.assertRaises(TestDataError):
            self.tdm.delete_pool(self.pool["id"])

    def test_stats_shape(self):
        self.tdm.borrow_item(self._item("u1")["id"], "张三")
        s = self.tdm.stats(self.pid)
        self.assertEqual(s["items_total"], 6)
        self.assertEqual(s["items_by_env"]["dev"]["borrowed"], 1)
        self.assertEqual(s["active_count"], 1)


if __name__ == "__main__":
    unittest.main()
