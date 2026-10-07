"""测试数据管理（TDM, Test Data Management）。

把散落在各条用例里、写死、用完不回收的测试数据收进统一的数据池：

- **数据池** :class:`TestDataManager` 按类别组织（账号、订单号、手机号……），
  池级声明支持的逻辑环境（dev / staging / prod）与默认借用期限；
- **数据项** 池中一条条可借用的数据，**按环境独立成条目**——同一份逻辑数据
  在开发、预发各有一行、各自有状态、各自加锁，借用必须指定环境，从根上
  杜绝开发 / 预发串用；
- **借用单** 一次借用生成一张单子，记录借用人、用途、借出 / 应还 / 实际
  归还时间，数据被借出期间对其它人锁定，归还（或被回收）后重新可用。

并发安全
--------
借用是「查可用 → 锁定数据 → 写借用单」的读-改-写序列，涉及多个分片存储，
单存储自带的文件锁无法覆盖整个序列。这里再叠加一把 **TDM 专用全局锁**
（进程内 ``threading.RLock`` + 跨进程 flock），把批量借用整体串行化，保证
两个测试 worker 同时申请同一批数据时不会拿到同一条。

过期治理
--------
:meth:`TestDataManager.sweep_overdue` 由调度器后台 tick 周期调用（幂等）：

- 超过应还时间仍未还的单子置为 ``overdue``，数据保持锁定；
- 按池策略 ``remind``（只提醒）或 ``reclaim``（宽限期后强制回收）处理，
  提醒按次数递进，全程写入借用单事件流，并可投递到项目的通知集成。
"""

from __future__ import annotations

import os
import threading
import time
from typing import Optional

from .models import (TDM_DATA_STATUSES, TDM_ENVIRONMENTS, TDM_LEASE_STATUSES,
                     TDM_OVERDUE_POLICIES, new_id)
from storage.lock import FileLock, lock_path_for

# 默认借用时长（秒）与各池可覆盖的缺省值
DEFAULT_TTL_SECONDS = 3600
# reclaim 策略下，首次过期后再宽限多久强制回收
DEFAULT_GRACE_SECONDS = 1800
# remind 策略下最多提醒次数，之后不再重复打扰
MAX_REMIND_COUNT = 3
# 再次提醒的最小间隔（秒），避免每个 tick 都提醒
REMIND_INTERVAL_SECONDS = 900


class TestDataError(ValueError):
    """TDM 业务校验错误（参数合法但业务上不允许）。"""


def _clean_tags(tags) -> list[str]:
    if not tags:
        return []
    if not isinstance(tags, (list, tuple)):
        raise TestDataError("tags 必须是字符串数组")
    out = []
    for t in tags:
        t = str(t).strip()
        if t and t not in out:
            out.append(t)
    return out


class TestDataManager:
    """测试数据池 / 数据项 / 借用单管理。"""

    def __init__(self, registry, data_root: str, notify_manager=None):
        self._pools = registry.store("tdm_pools")
        self._items = registry.store("tdm_items")
        self._leases = registry.store("tdm_leases")
        self._notify = notify_manager
        # 进程内串行化所有借用 / 归还 / 回收
        self._mem_lock = threading.RLock()
        # 跨进程锁：与数据文件解耦的专用锁文件
        self._lock_path = os.path.join(data_root, "tdm", "borrow.lock")

    # ---------------------------------------------------------------- 工具
    def _locked(self):
        return FileLock(lock_path_for(self._lock_path))

    def _get_pool_or_raise(self, pool_id: str) -> dict:
        pool = self._pools.get(pool_id)
        if pool is None:
            raise TestDataError("数据池不存在")
        return pool

    def _check_env(self, pool: dict, env: str) -> None:
        if env not in (pool.get("envs") or TDM_ENVIRONMENTS):
            raise TestDataError(
                f"环境 {env} 不在数据池「{pool.get('name')}」支持的环境"
                f"{pool.get('envs')} 内（多环境隔离）")

    @staticmethod
    def _match_tags(item_tags: list, wanted) -> bool:
        """标签筛选：要求全部命中（AND）；不传表示不限制。"""
        if not wanted:
            return True
        return all(t in (item_tags or []) for t in wanted)

    def _fire(self, project_id: str, event: str, payload: dict) -> None:
        """投递到项目的通知集成；通知失败绝不影响主流程。"""
        if self._notify is None or not project_id:
            return
        try:
            self._notify.fire(project_id, event, payload)
        except Exception:  # noqa: BLE001
            pass

    # ---------------------------------------------------------------- 数据池
    def create_pool(self, project_id: str, payload: dict) -> dict:
        name = (payload.get("name") or "").strip()
        if not name:
            raise TestDataError("数据池名称不能为空")
        envs = payload.get("envs") or list(TDM_ENVIRONMENTS)
        bad = [e for e in envs if e not in TDM_ENVIRONMENTS]
        if bad:
            raise TestDataError(f"未知环境: {bad}，可选 {TDM_ENVIRONMENTS}")
        policy = payload.get("overdue_policy", "remind")
        if policy not in TDM_OVERDUE_POLICIES:
            policy = "remind"
        pool = {
            "id": new_id("pool"),
            "project_id": project_id,
            "name": name,
            "category": payload.get("category", "").strip() or name,
            "description": payload.get("description", ""),
            "envs": envs,
            "default_ttl": int(payload.get("default_ttl") or DEFAULT_TTL_SECONDS),
            "overdue_policy": policy,
            "grace_seconds": int(payload.get("grace_seconds")
                                 if payload.get("grace_seconds") is not None
                                 else DEFAULT_GRACE_SECONDS),
            "tags": _clean_tags(payload.get("tags")),
        }
        self._pools.insert(pool)
        return pool

    def list_pools(self, project_id: str) -> list[dict]:
        pools = self._pools.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="asc")
        # 附带条目 / 可用计数（含环境维度），列表页直接展示
        items = self._items.query(where=[("project_id", "eq", project_id)])
        active = {l.get("item_id") for l in self._list_active_lease_ids(project_id)}
        for p in pools:
            pool_items = [i for i in items if i.get("pool_id") == p["id"]]
            by_env = {e: {"total": 0, "available": 0} for e in (p.get("envs") or [])}
            for i in pool_items:
                env = i.get("env")
                slot = by_env.setdefault(env, {"total": 0, "available": 0})
                slot["total"] += 1
                if i.get("status") == "available" and i["id"] not in active:
                    slot["available"] += 1
            p["item_count"] = len(pool_items)
            p["available_count"] = sum(s["available"] for s in by_env.values())
            p["by_env"] = by_env
        return pools

    def get_pool(self, pool_id: str) -> Optional[dict]:
        return self._pools.get(pool_id)

    def update_pool(self, pool_id: str, patch: dict) -> dict:
        pool = self._get_pool_or_raise(pool_id)
        out = {}
        for k in ("name", "category", "description", "default_ttl",
                  "overdue_policy", "grace_seconds", "tags"):
            if k in patch:
                out[k] = patch[k]
        if "envs" in patch:
            envs = patch["envs"] or []
            bad = [e for e in envs if e not in TDM_ENVIRONMENTS]
            if bad:
                raise TestDataError(f"未知环境: {bad}")
            # 不允许移除仍有数据的环境，避免环境轴上的数据「悬空」
            used = {i.get("env") for i in self._items.query(
                where=[("pool_id", "eq", pool_id)])}
            removed = used - set(envs)
            if removed:
                raise TestDataError(f"环境 {sorted(removed)} 下仍有数据，不能移除")
            out["envs"] = envs
        if "tags" in out:
            out["tags"] = _clean_tags(out["tags"])
        if out.get("overdue_policy") and out["overdue_policy"] not in TDM_OVERDUE_POLICIES:
            raise TestDataError("overdue_policy 只能是 remind / reclaim")
        updated = self._pools.update(pool_id, out)
        return updated if updated is not None else pool

    def delete_pool(self, pool_id: str) -> bool:
        pool = self._get_pool_or_raise(pool_id)
        items = self._items.query(where=[("pool_id", "eq", pool_id)])
        locked = [i for i in items if i.get("status") == "borrowed"]
        if locked:
            raise TestDataError(f"池内还有 {len(locked)} 条数据被借出，请先归还再删除")
        for i in items:
            self._items.delete(i["id"])
        for lease in self._leases.query(where=[("pool_id", "eq", pool_id)]):
            self._leases.delete(lease["id"])
        return self._pools.delete(pool_id)

    # ---------------------------------------------------------------- 数据项
    def _build_item(self, pool: dict, project_id: str, raw: dict) -> dict:
        env = raw.get("env") or pool.get("envs", [None])[0]
        self._check_env(pool, env)
        key = (raw.get("data_key") or raw.get("key") or "").strip()
        if not key:
            raise TestDataError("data_key 不能为空（如账号 / 订单号）")
        payload = raw.get("payload")
        if payload is None:
            # 允许直接给 value / attributes 两种简写
            payload = raw.get("attributes")
        if payload is None:
            payload = {"value": raw.get("value", key)}
        status = raw.get("status", "available")
        if status not in TDM_DATA_STATUSES:
            status = "available"
        return {
            "id": new_id("tdata"),
            "project_id": project_id,
            "pool_id": pool["id"],
            "env": env,
            "data_key": key,
            "payload": payload,
            "tags": _clean_tags(raw.get("tags")),
            "status": status,
            "description": raw.get("description", ""),
        }

    def import_items(self, pool_id: str, items: list,
                     upsert: bool = False) -> dict:
        """批量导入数据。同 (env, data_key) 视为重复：跳过或更新。"""
        if not isinstance(items, list) or not items:
            raise TestDataError("items 必须是非空数组")
        pool = self._get_pool_or_raise(pool_id)
        with self._mem_lock, self._locked():
            existing = self._items.query(where=[("pool_id", "eq", pool_id)])
            index = {(i.get("env"), i.get("data_key")): i for i in existing}
            created, updated, skipped = [], [], []
            for raw in items:
                item = self._build_item(pool, pool["project_id"], raw)
                sig = (item["env"], item["data_key"])
                old = index.get(sig)
                if old is None:
                    item_id = self._items.insert(item)
                    created.append(item_id)
                    index[sig] = {"id": item_id}
                elif upsert and old.get("status") == "available":
                    self._items.update(old["id"], {
                        "payload": item["payload"],
                        "tags": item["tags"],
                        "description": item["description"],
                    })
                    updated.append(old["id"])
                else:
                    skipped.append(sig)
        return {"pool_id": pool_id, "created": len(created),
                "updated": len(updated), "skipped": len(skipped),
                "skipped_keys": [f"{e}:{k}" for e, k in skipped]}

    def add_item(self, pool_id: str, raw: dict) -> dict:
        result = self.import_items(pool_id, [raw])
        if result["skipped"]:
            raise TestDataError(f"数据已存在: {result['skipped_keys'][0]}")
        return self._items.get(result["created"][0])

    def _attach_lease(self, items: list[dict]) -> list[dict]:
        if not items:
            return items
        ids = [i["id"] for i in items]
        leases = self._leases.query(where=[("status", "in", ["active", "overdue"])])
        current = {l["item_id"]: l for l in leases if l.get("item_id") in ids}
        now_ts = time.time()
        for i in items:
            lease = current.get(i["id"])
            if lease:
                i["current_lease"] = self._lease_view(lease, now_ts=now_ts)
            else:
                i["current_lease"] = None
        return items

    def list_items(self, project_id: str = None, pool_id: str = None,
                   env: str = None, status: str = None, tag: str = None,
                   q: str = None) -> list[dict]:
        where = []
        if project_id:
            where.append(("project_id", "eq", project_id))
        if pool_id:
            where.append(("pool_id", "eq", pool_id))
        if env:
            where.append(("env", "eq", env))
        if status:
            where.append(("status", "eq", status))
        if tag:
            where.append(("tags", "contains", tag))
        items = self._items.query(where=where or None,
                                  order_by="created_at", order="asc")
        if q:
            needle = q.lower()
            items = [i for i in items
                     if needle in (i.get("data_key", "") +
                                   str(i.get("payload", ""))).lower()]
        return self._attach_lease(items)

    def get_item(self, item_id: str) -> Optional[dict]:
        items = self._attach_lease([self._items.get(item_id)])
        return items[0] if items and items[0] else None

    def update_item(self, item_id: str, patch: dict) -> dict:
        item = self._items.get(item_id)
        if item is None:
            raise TestDataError("数据不存在")
        out = {}
        for k in ("payload", "description", "tags", "data_key"):
            if k in patch:
                out[k] = patch[k]
        if "tags" in out:
            out["tags"] = _clean_tags(out["tags"])
        if "status" in patch:
            status = patch["status"]
            if status not in TDM_DATA_STATUSES:
                raise TestDataError(f"状态需为 {TDM_DATA_STATUSES}")
            if status == "available" and item.get("status") == "borrowed":
                raise TestDataError("借出的数据不能直接改为可用，请走归还流程")
            out["status"] = status
        updated = self._items.update(item_id, out)
        return updated if updated is not None else item

    def delete_item(self, item_id: str) -> bool:
        item = self._items.get(item_id)
        if item is None:
            return False
        if item.get("status") == "borrowed":
            raise TestDataError("数据被借出中，请先归还再删除")
        return self._items.delete(item_id)

    # ---------------------------------------------------------------- 借用
    def _make_lease(self, item: dict, borrower: str, ttl: int,
                    purpose: str, source: str) -> dict:
        now_ts = time.time()
        return {
            "id": new_id("lease"),
            "project_id": item["project_id"],
            "pool_id": item["pool_id"],
            "item_id": item["id"],
            "env": item["env"],
            "data_key": item["data_key"],
            "borrower": borrower,
            "purpose": purpose,
            "source": source,  # manual / api / case:<case_id>
            "borrowed_at": now_ts,
            "due_at": now_ts + max(60, int(ttl)),
            "returned_at": None,
            "status": "active",
            "remind_count": 0,
            "last_reminded_at": None,
            "events": [{
                "at": now_ts, "type": "borrow",
                "message": f"{borrower} 借用（{item['env']} 环境），"
                           f"应还 {time.strftime('%m-%d %H:%M', time.localtime(now_ts + max(60, int(ttl))))}",
            }],
        }

    def borrow_item(self, item_id: str, borrower: str,
                    ttl: int = None, purpose: str = "",
                    source: str = "manual") -> dict:
        """借用指定单条数据；数据非可用状态即失败（锁定语义）。"""
        if not borrower:
            raise TestDataError("借用人不能为空")
        with self._mem_lock, self._locked():
            item = self._items.get(item_id)
            if item is None:
                raise TestDataError("数据不存在")
            if item.get("status") != "available":
                view = self.get_item(item_id)
                cur = (view or {}).get("current_lease") or {}
                raise TestDataError(
                    f"数据已被 {cur.get('borrower', '?')} 借走，当前锁定中")
            pool = self._get_pool_or_raise(item["pool_id"])
            ttl = int(ttl or pool.get("default_ttl") or DEFAULT_TTL_SECONDS)
            lease = self._make_lease(item, borrower, ttl, purpose, source)
            self._leases.insert(lease)
            self._items.update(item_id, {"status": "borrowed"})
            self._fire(item["project_id"], "tdm.borrowed",
                       self._notice_payload(lease))
            return self.get_lease(lease["id"])

    def borrow(self, pool_id: str, borrower: str, env: str,
               count: int = 1, ttl: int = None, purpose: str = "",
               tags=None, source: str = "manual",
               partial: bool = False) -> dict:
        """按条件从池中申请借用若干条数据。

        标签要求 AND 命中；只在指定环境中挑选（多环境隔离）。
        ``partial=False``（默认）时数量不足整单失败、一条不借；
        ``partial=True`` 时有多少借多少。
        """
        if not borrower:
            raise TestDataError("借用人不能为空")
        count = int(count or 1)
        if count <= 0:
            raise TestDataError("借用数量必须大于 0")
        with self._mem_lock, self._locked():
            pool = self._get_pool_or_raise(pool_id)
            self._check_env(pool, env)
            ttl = int(ttl or pool.get("default_ttl") or DEFAULT_TTL_SECONDS)
            wanted_tags = tags or []

            # 已借出（active / overdue）的数据视为锁定，即便状态字段滞后也排除
            locked = {l.get("item_id") for l in self._leases.query(
                where=[("status", "in", ["active", "overdue"]),
                       ("pool_id", "eq", pool_id), ("env", "eq", env)])}
            candidates = []
            for item in self._items.query(where=[("pool_id", "eq", pool_id),
                                                 ("env", "eq", env),
                                                 ("status", "eq", "available")],
                                          order_by="created_at", order="asc"):
                if item["id"] in locked:
                    continue
                if self._match_tags(item.get("tags"), wanted_tags):
                    candidates.append(item)

            if len(candidates) < count and not partial:
                raise TestDataError(
                    f"{env} 环境满足条件的可用数据不足：需要 {count}，"
                    f"仅剩 {len(candidates)}")
            chosen = candidates[:count]
            leases = []
            for item in chosen:
                lease = self._make_lease(item, borrower, ttl, purpose, source)
                self._leases.insert(lease)
                self._items.update(item["id"], {"status": "borrowed"})
                leases.append(lease)
            project_id = pool.get("project_id")
            self._fire(project_id, "tdm.borrowed", {
                "pool_id": pool_id, "env": env, "borrower": borrower,
                "count": len(leases), "purpose": purpose,
                "lease_ids": [l["id"] for l in leases],
            })
            return {
                "pool_id": pool_id, "env": env,
                "requested": count, "borrowed": len(leases),
                "leases": [self._lease_view(l) for l in leases],
            }

    # ---------------------------------------------------------------- 归还 / 回收
    def return_lease(self, lease_id: str, borrower: str = None,
                     note: str = "") -> dict:
        with self._mem_lock, self._locked():
            lease = self._leases.get(lease_id)
            if lease is None:
                raise TestDataError("借用单不存在")
            if lease.get("status") not in ("active", "overdue"):
                raise TestDataError("借用单已结束，无需归还")
            if borrower and lease.get("borrower") != borrower:
                raise TestDataError(
                    f"只有借用人 {lease.get('borrower')} 本人可以归还")
            return self._close_lease(lease, "returned",
                                     actor=borrower or lease.get("borrower"),
                                     note=note or "用后归还")

    def return_by_borrower(self, project_id: str, borrower: str,
                           pool_id: str = None, env: str = None) -> dict:
        """一键归还某借用人（可限定池 / 环境）名下全部未结借用单。"""
        if not borrower:
            raise TestDataError("借用人不能为空")
        with self._mem_lock, self._locked():
            where = [("project_id", "eq", project_id),
                     ("borrower", "eq", borrower),
                     ("status", "in", ["active", "overdue"])]
            if pool_id:
                where.append(("pool_id", "eq", pool_id))
            if env:
                where.append(("env", "eq", env))
            leases = self._leases.query(where=where)
            ids = []
            for lease in leases:
                closed = self._close_lease(lease, "returned",
                                           actor=borrower, note="批量归还")
                ids.append(closed["id"])
            return {"borrower": borrower, "returned": len(ids), "lease_ids": ids}

    def force_return(self, lease_id: str, operator: str = "admin",
                     reason: str = "管理员强制回收") -> dict:
        """管理员强制回收（不校验借用人），数据立即恢复可用。"""
        with self._mem_lock, self._locked():
            lease = self._leases.get(lease_id)
            if lease is None:
                raise TestDataError("借用单不存在")
            if lease.get("status") not in ("active", "overdue"):
                raise TestDataError("借用单已结束")
            return self._close_lease(lease, "reclaimed", actor=operator,
                                     note=reason, force=True)

    def renew(self, lease_id: str, extra_seconds: int = None,
              actor: str = None) -> dict:
        """续借：延长应还时间；若已过期，续借后恢复 active。"""
        with self._mem_lock, self._locked():
            lease = self._leases.get(lease_id)
            if lease is None:
                raise TestDataError("借用单不存在")
            if lease.get("status") not in ("active", "overdue"):
                raise TestDataError("借用单已结束，无法续借")
            if actor and lease.get("borrower") != actor:
                raise TestDataError("只有借用人本人可以续借")
            pool = self._pools.get(lease.get("pool_id"))
            extra = int(extra_seconds or
                        (pool or {}).get("default_ttl") or DEFAULT_TTL_SECONDS)
            events = list(lease.get("events") or [])
            if lease.get("status") == "overdue":
                events.append({"at": time.time(), "type": "renew",
                               "message": f"续借 {extra}s，过期状态解除"})
                patch = {"status": "active", "remind_count": 0,
                         "last_reminded_at": None}
            else:
                events.append({"at": time.time(), "type": "renew",
                               "message": f"续借 {extra}s"})
                patch = {}
            patch["due_at"] = lease.get("due_at", time.time()) + max(60, extra)
            patch["events"] = events
            updated = self._leases.update(lease_id, patch)
            return self._lease_view(updated or lease)

    def _close_lease(self, lease: dict, final_status: str,
                     actor: str, note: str, force: bool = False) -> dict:
        now_ts = time.time()
        events = list(lease.get("events") or [])
        events.append({
            "at": now_ts,
            "type": "force_return" if force else "return",
            "actor": actor,
            "message": note,
        })
        updated = self._leases.update(lease["id"], {
            "status": final_status,
            "returned_at": now_ts,
            "events": events,
        })
        self._items.update(lease["item_id"], {"status": "available"})
        event = "tdm.reclaimed" if force else "tdm.returned"
        self._fire(lease.get("project_id"), event,
                   self._notice_payload(updated or lease))
        return self._lease_view(updated or lease)

    # ---------------------------------------------------------------- 查询
    def _lease_view(self, lease: dict, now_ts: float = None) -> dict:
        """给借用单附上数据快照与已借时长，供前端直接渲染。"""
        view = dict(lease)
        item = self._items.get(lease.get("item_id"))
        if item:
            view["data_key"] = item.get("data_key", lease.get("data_key"))
            view["payload"] = item.get("payload")
            view["tags"] = item.get("tags")
        pool = self._pools.get(lease.get("pool_id"))
        view["pool_name"] = pool.get("name") if pool else None
        now_ts = now_ts if now_ts is not None else time.time()
        end = lease.get("returned_at") or now_ts
        view["held_seconds"] = max(
            0, round(end - lease.get("borrowed_at", end), 1))
        view["overdue_seconds"] = (
            max(0, round(now_ts - lease.get("due_at", now_ts), 1))
            if lease.get("status") in ("active", "overdue") and now_ts > lease.get("due_at", now_ts)
            else 0)
        return view

    def list_leases(self, project_id: str, status: str = None,
                    borrower: str = None, pool_id: str = None,
                    env: str = None, item_id: str = None,
                    limit: int = 200) -> list[dict]:
        where = [("project_id", "eq", project_id)]
        if status:
            where.append(("status", "eq", status))
        if borrower:
            where.append(("borrower", "eq", borrower))
        if pool_id:
            where.append(("pool_id", "eq", pool_id))
        if env:
            where.append(("env", "eq", env))
        if item_id:
            where.append(("item_id", "eq", item_id))
        leases = self._leases.query(where=where, order_by="borrowed_at",
                                    order="desc", limit=limit)
        return [self._lease_view(l) for l in leases]

    def get_lease(self, lease_id: str) -> Optional[dict]:
        lease = self._leases.get(lease_id)
        return self._lease_view(lease) if lease else None

    def item_history(self, item_id: str) -> dict:
        item = self._items.get(item_id)
        if item is None:
            raise TestDataError("数据不存在")
        leases = self._leases.query(where=[("item_id", "eq", item_id)],
                                    order_by="borrowed_at", order="desc")
        return {"item": self.get_item(item_id),
                "leases": [self._lease_view(l) for l in leases]}

    def _list_active_lease_ids(self, project_id: str) -> list[dict]:
        return self._leases.query(where=[("project_id", "eq", project_id),
                                         ("status", "in", ["active", "overdue"])])

    # ---------------------------------------------------------------- 过期治理
    def sweep_overdue(self, now_ts: float = None) -> dict:
        """扫描全部未结借用单：置过期、递进提醒、按策略强制回收。幂等。"""
        now_ts = now_ts if now_ts is not None else time.time()
        result = {"marked_overdue": 0, "reminded": 0, "reclaimed": 0}
        with self._mem_lock, self._locked():
            leases = self._leases.query(
                where=[("status", "in", ["active", "overdue"])])
            for lease in leases:
                due = lease.get("due_at", 0)
                if now_ts <= due:
                    continue
                pool = self._pools.get(lease.get("pool_id")) or {}
                policy = pool.get("overdue_policy", "remind")

                # 1) 首次发现过期：active -> overdue
                if lease.get("status") != "overdue":
                    lease = self._leases.update(lease["id"], {
                        "status": "overdue",
                        "events": list(lease.get("events") or []) + [{
                            "at": now_ts, "type": "overdue",
                            "message": "超过应还时间未归还，已标记过期",
                        }],
                    })
                    result["marked_overdue"] += 1

                # 2) reclaim 策略：宽限期满强制回收
                if policy == "reclaim" and now_ts >= due + int(
                        pool.get("grace_seconds", DEFAULT_GRACE_SECONDS)):
                    closed = self._close_lease(
                        lease, "reclaimed", actor="system",
                        note="超过应还时间且宽限期满，系统自动强制回收",
                        force=True)
                    result["reclaimed"] += 1
                    continue

                # 3) 提醒节流：达到上限或距上次提醒不足间隔则跳过
                count = int(lease.get("remind_count") or 0)
                if count >= MAX_REMIND_COUNT:
                    continue
                last = lease.get("last_reminded_at") or 0
                if last and now_ts - last < REMIND_INTERVAL_SECONDS:
                    continue
                count += 1
                events = list(lease.get("events") or []) + [{
                    "at": now_ts, "type": "remind",
                    "message": f"过期提醒（第 {count} 次），请尽快归还",
                }]
                lease = self._leases.update(lease["id"], {
                    "remind_count": count,
                    "last_reminded_at": now_ts,
                    "events": events,
                })
                self._fire(lease.get("project_id"), "tdm.overdue_reminder",
                           self._notice_payload(lease, remind_count=count))
                result["reminded"] += 1
        return result

    def _notice_payload(self, lease: dict, remind_count: int = None) -> dict:
        now_ts = time.time()
        return {
            "lease_id": lease.get("id"),
            "project_id": lease.get("project_id"),
            "pool_id": lease.get("pool_id"),
            "item_id": lease.get("item_id"),
            "data_key": lease.get("data_key"),
            "env": lease.get("env"),
            "borrower": lease.get("borrower"),
            "purpose": lease.get("purpose"),
            "borrowed_at": lease.get("borrowed_at"),
            "due_at": lease.get("due_at"),
            "overdue_seconds": max(0, round(now_ts - lease.get("due_at", now_ts), 1)),
            "remind_count": remind_count if remind_count is not None
                            else lease.get("remind_count"),
            "status": lease.get("status"),
        }

    # ---------------------------------------------------------------- 统计
    def stats(self, project_id: str) -> dict:
        items = self._items.query(where=[("project_id", "eq", project_id)])
        leases = self._leases.query(where=[("project_id", "eq", project_id)])
        by_env: dict[str, dict] = {}
        by_status: dict[str, int] = {}
        tag_counter: dict[str, int] = {}
        now_ts = time.time()
        for i in items:
            env_slot = by_env.setdefault(i.get("env"), {
                "total": 0, "available": 0, "borrowed": 0, "retired": 0})
            env_slot["total"] += 1
            env_slot[i.get("status", "available")] = \
                env_slot.get(i.get("status", "available"), 0) + 1
            by_status[i.get("status", "available")] = \
                by_status.get(i.get("status", "available"), 0) + 1
            for t in i.get("tags") or []:
                tag_counter[t] = tag_counter.get(t, 0) + 1

        lease_by_status: dict[str, int] = {}
        overdue = 0
        for l in leases:
            st = l.get("status", "active")
            lease_by_status[st] = lease_by_status.get(st, 0) + 1
            if st in ("active", "overdue") and now_ts > l.get("due_at", 0):
                overdue += 1
        return {
            "pools": len(self._pools.query(
                where=[("project_id", "eq", project_id)])),
            "items_total": len(items),
            "items_by_status": by_status,
            "items_by_env": by_env,
            "tag_counts": tag_counter,
            "leases_total": len(leases),
            "leases_by_status": lease_by_status,
            "active_count": lease_by_status.get("active", 0),
            "overdue_count": overdue,
        }
