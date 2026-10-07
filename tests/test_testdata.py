"""测试数据管理引擎测试。

覆盖：数据池 CRUD、批量导入去重、借用/锁定/归还、环境隔离、标签条件、
并发借用互斥、过期提醒、过期强制回收、借用历史与统计。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import NotificationManager, TestDataManager  # noqa: E402
from storage import StoreRegistry  # noqa: E402


class TestDataBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = StoreRegistry(os.path.join(self.tmp.name, "store"))
        self.notify = NotificationManager(self.registry)
        self.mgr = TestDataManager(self.registry, self.notify)
        self.pool = self.mgr.create_pool("p1", {
            "name": "测试账号池", "category": "账号",
            "envs": ["dev", "staging"],
            "default_lease_minutes": 60, "overdue_action": "notify",
        })

    def tearDown(self):
        self.tmp.cleanup()

    def _add(self, key, env="dev", tags=None, value=None):
        return self.mgr.add_item(self.pool["id"], {
            "key": key, "env": env, "tags": tags or [],
            "value": value if value is not None else {"v": key},
        })


class TestPoolCrud(TestDataBase):
    def test_create_and_list(self):
        pools = self.mgr.list_pools("p1")
        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0]["name"], "测试账号池")
        self.assertEqual(pools[0]["counts"]["total"], 0)

    def test_create_requires_name(self):
        self.assertIn("error", self.mgr.create_pool("p1", {"name": "  "}))

    def test_delete_nonempty_pool_rejected(self):
        self._add("u1")
        result = self.mgr.delete_pool(self.pool["id"])
        self.assertIn("error", result)
        # 清空后可以删
        item = self.mgr.list_items("p1")[0]
        self.mgr.delete_item(item["id"])
        self.assertTrue(self.mgr.delete_pool(self.pool["id"])["deleted"])

    def test_update_pool(self):
        updated = self.mgr.update_pool(self.pool["id"],
                                       {"overdue_action": "reclaim"})
        self.assertEqual(updated["overdue_action"], "reclaim")


class TestItems(TestDataBase):
    def test_add_and_dup_key(self):
        item = self._add("qa_admin", tags=["登录"])
        self.assertEqual(item["status"], "available")
        dup = self._add("qa_admin")
        self.assertIn("error", dup)

    def test_same_key_different_env_ok(self):
        self.assertNotIn("error", self._add("qa", env="dev"))
        self.assertNotIn("error", self._add("qa", env="staging"))

    def test_env_outside_pool_rejected(self):
        result = self._add("qa", env="prod")
        self.assertIn("error", result)

    def test_import_dedup_and_skip(self):
        r1 = self.mgr.import_items(self.pool["id"], [
            {"key": "a"}, {"key": "b"}, {"key": "c"}], env="dev", tags=["批量"])
        self.assertEqual(r1["imported"], 3)
        # 重复导入：全部跳过而不是失败
        r2 = self.mgr.import_items(self.pool["id"], [{"key": "a"}, {"key": "b"}],
                                   env="dev")
        self.assertEqual(r2["imported"], 0)
        self.assertEqual(r2["skipped"], 2)
        items = self.mgr.list_items("p1", tag="批量")
        self.assertEqual(len(items), 3)

    def test_list_filters(self):
        self._add("u1", env="dev", tags=["登录"])
        self._add("u2", env="staging", tags=["支付"])
        self.assertEqual(len(self.mgr.list_items("p1", env="dev")), 1)
        self.assertEqual(len(self.mgr.list_items("p1", tag="支付")), 1)
        self.assertEqual(len(self.mgr.list_items("p1", q="u2")), 1)

    def test_delete_in_use_rejected(self):
        item = self._add("u1")
        self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        self.assertIn("error", self.mgr.delete_item(item["id"]))
        # 借出中也禁止直接停用
        self.assertIn("error", self.mgr.update_item(item["id"],
                                                    {"status": "disabled"}))


class TestBorrowReturn(TestDataBase):
    def test_borrow_locks_item(self):
        item = self._add("u1")
        loan = self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        self.assertIn("id", loan)
        self.assertEqual(loan["item_key"], "u1")
        self.assertEqual(loan["lease_minutes"], 60)
        # 数据被锁定，状态 in_use
        locked = self.mgr.get_item(item["id"])
        self.assertEqual(locked["status"], "in_use")
        self.assertEqual(locked["borrowed_by"], "zhangsan")
        # 再借：没有空闲数据
        again = self.mgr.borrow("p1", self.pool["id"], "lisi", env="dev")
        self.assertIn("error", again)

    def test_return_makes_available_and_counts(self):
        item = self._add("u1")
        loan = self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        returned = self.mgr.return_item(item_id=item["id"])
        self.assertEqual(returned["status"], "returned")
        self.assertIsNotNone(returned["duration"])
        freed = self.mgr.get_item(item["id"])
        self.assertEqual(freed["status"], "available")
        self.assertEqual(freed["borrow_count"], 1)
        self.assertIsNone(freed["borrowed_by"])
        # 归还后别人可以再借
        loan2 = self.mgr.borrow("p1", self.pool["id"], "lisi", env="dev")
        self.assertIn("id", loan2)
        # 重复归还报错
        self.assertIn("error", self.mgr.return_item(loan_id=loan["id"]))

    def test_borrow_specific_item(self):
        self._add("u1")
        item2 = self._add("u2")
        loan = self.mgr.borrow("p1", self.pool["id"], "zhangsan",
                               item_id=item2["id"])
        self.assertEqual(loan["item_key"], "u2")

    def test_borrow_by_tags(self):
        self._add("u1", tags=["登录"])
        self._add("u2", tags=["登录", "支付"])
        loan = self.mgr.borrow("p1", self.pool["id"], "zhangsan",
                               env="dev", tags=["支付"])
        self.assertEqual(loan["item_key"], "u2")
        # 条件不满足时报错
        none = self.mgr.borrow("p1", self.pool["id"], "lisi",
                               env="dev", tags=["不存在的标签"])
        self.assertIn("error", none)

    def test_env_isolation(self):
        """dev 的借用只分 dev 的数据，staging 互不串用。"""
        self._add("dev_user", env="dev")
        self._add("stg_user", env="staging")
        loan = self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="staging")
        self.assertEqual(loan["item_key"], "stg_user")
        self.assertEqual(loan["env"], "staging")
        # dev 数据还在，staging 已借空
        self.assertIn("error", self.mgr.borrow("p1", self.pool["id"], "lisi",
                                               env="staging"))
        self.assertIn("id", self.mgr.borrow("p1", self.pool["id"], "lisi",
                                            env="dev"))
        # 指定 staging 数据但要求 dev 环境 → 拒绝
        stg_item = self.mgr.list_items("p1", env="staging")[0]
        cross = self.mgr.borrow("p1", self.pool["id"], "wangwu",
                                env="dev", item_id=stg_item["id"])
        self.assertIn("error", cross)

    def test_borrow_requires_borrower(self):
        self._add("u1")
        self.assertIn("error", self.mgr.borrow("p1", self.pool["id"], " "))

    def test_concurrent_borrow_only_one_wins(self):
        """并发抢同一条数据：只有一个成功，其余收到「已被借出」。"""
        item = self._add("u1")
        results = []
        barrier = threading.Barrier(8)

        def race():
            barrier.wait()
            results.append(self.mgr.borrow("p1", self.pool["id"], "t",
                                           item_id=item["id"]))

        threads = [threading.Thread(target=race) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wins = [r for r in results if "id" in r]
        self.assertEqual(len(wins), 1)
        self.assertEqual(self.mgr.get_item(item["id"])["status"], "in_use")

    def test_concurrent_auto_allocate_no_dup(self):
        """并发自动分配：N 条数据被 N 个请求借走，互不重复。"""
        for i in range(5):
            self._add(f"u{i}")
        loans = []
        barrier = threading.Barrier(5)

        def race():
            barrier.wait()
            r = self.mgr.borrow("p1", self.pool["id"], "t", env="dev")
            if "id" in r:
                loans.append(r)

        threads = [threading.Thread(target=race) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(loans), 5)
        self.assertEqual(len({l["item_id"] for l in loans}), 5)


class TestOverdue(TestDataBase):
    def _borrow_overdue(self, borrower="zhangsan", minutes_ago=90):
        loan = self.mgr.borrow("p1", self.pool["id"], borrower, env="dev")
        past = time.time() - minutes_ago * 60
        self.registry.store("data_loans").update(
            loan["id"], {"borrowed_at": past, "due_at": past + 60 * 60})
        return loan

    def test_overdue_notify_once(self):
        self._add("u1")
        self.notify.create("p1", {"type": "webhook",
                                  "config": {"url": "http://x"},
                                  "events": ["testdata.overdue"]})
        loan = self._borrow_overdue()
        r1 = self.mgr.check_overdue()
        self.assertEqual(r1["reminded"], 1)
        # 数据仍锁定（仅提醒不回收）
        self.assertEqual(self.mgr.list_items("p1")[0]["status"], "in_use")
        # 通知事件已投递
        events = self.notify.events("p1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "testdata.overdue")
        # 再次扫描不重复提醒
        r2 = self.mgr.check_overdue()
        self.assertEqual(r2["reminded"], 0)
        self.assertEqual(len(self.notify.events("p1")), 1)
        self.assertTrue(self.mgr.list_loans("p1")[0]["overdue"])
        self.assertTrue(self.registry.store("data_loans").get(loan["id"])["reminded"])

    def test_overdue_force_reclaim(self):
        self.mgr.update_pool(self.pool["id"], {"overdue_action": "reclaim"})
        item = self._add("u1")
        self.notify.create("p1", {"type": "webhook",
                                  "config": {"url": "http://x"},
                                  "events": ["testdata.reclaimed"]})
        self._borrow_overdue()
        r = self.mgr.check_overdue()
        self.assertEqual(r["reclaimed"], 1)
        # 数据被回收，重新可用
        freed = self.mgr.get_item(item["id"])
        self.assertEqual(freed["status"], "available")
        loan = self.mgr.list_loans("p1")[0]
        self.assertEqual(loan["status"], "force_reclaimed")
        self.assertEqual(loan["reclaimed_by"], "system(过期自动回收)")
        self.assertEqual(self.notify.events("p1")[0]["event"],
                         "testdata.reclaimed")
        # 回收后可以再借
        self.assertIn("id", self.mgr.borrow("p1", self.pool["id"], "lisi",
                                            env="dev"))

    def test_not_overdue_untouched(self):
        self._add("u1")
        self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        r = self.mgr.check_overdue()
        self.assertEqual(r, {"reminded": 0, "reclaimed": 0})

    def test_manual_reclaim(self):
        item = self._add("u1")
        self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        result = self.mgr.reclaim(item_id=item["id"], operator="admin")
        self.assertEqual(result["status"], "force_reclaimed")
        self.assertEqual(self.mgr.get_item(item["id"])["status"], "available")


class TestHistoryAndStats(TestDataBase):
    def test_history_tracks_all_loans(self):
        item = self._add("u1")
        l1 = self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        self.mgr.return_item(loan_id=l1["id"])
        self.mgr.borrow("p1", self.pool["id"], "lisi", env="dev")
        history = self.mgr.item_history(item["id"])
        self.assertEqual(len(history), 2)
        borrowers = {h["borrower"] for h in history}
        self.assertEqual(borrowers, {"zhangsan", "lisi"})
        # 已还的那条有时长，借出中的那条还是 active
        done = [h for h in history if h["status"] == "returned"]
        self.assertEqual(len(done), 1)
        self.assertIsNotNone(done[0]["duration"])

    def test_stats(self):
        self._add("u1", env="dev")
        self._add("u2", env="staging")
        self._add("u3", env="dev")
        self.mgr.borrow("p1", self.pool["id"], "zhangsan", env="dev")
        s = self.mgr.stats("p1")
        self.assertEqual(s["total"], 3)
        self.assertEqual(s["by_status"]["in_use"], 1)
        self.assertEqual(s["by_status"]["available"], 2)
        self.assertEqual(s["by_env"]["dev"], 2)
        self.assertEqual(s["by_env"]["staging"], 1)
        self.assertEqual(s["active_loans"], 1)


if __name__ == "__main__":
    unittest.main()
