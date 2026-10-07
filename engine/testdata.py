"""测试数据管理：数据池 + 借用 / 归还 / 回收全生命周期。

解决团队测试数据散落在各条用例里、账号被占不还、订单号写死撞车的问题：

- **数据池**：按类别（账号 / 订单号 / 手机号…）把数据组织成池，
  池上定义可用环境列表、默认借期与过期策略；
- **批量导入**：一次导入多条数据（``key=value`` 行或完整 dict），
  同池同环境下按 key 去重，重复导入自动跳过；
- **借用**：按「池 + 环境 + 标签」条件自动分配一条空闲数据，或指定
  具体数据；分配在进程内锁 + 状态复核下完成，并发借用不会拿到同一条；
- **锁定 / 归还**：借出期间状态为 ``in_use`` 对其他人不可见可用，
  归还后回到 ``available`` 重新进入可借队列；
- **追踪**：每条数据记录当前借用人、借出时间、累计借用次数，
  每次借用生成一条借用记录（loan），可查询完整历史与借了多久；
- **过期处理**：借用有到期时间（``due_at``），后台周期扫描过期未还
  的借用，按池策略「仅提醒」（发通知事件，一次）或「强制回收」
  （数据立即回收重新可用，并通知）；
- **多环境隔离**：每条数据归属一个环境（dev / staging…），借用必须
  指定环境，只在本环境内分配，互不串用。

存储：``data_pools`` / ``data_items`` / ``data_loans`` 三个分片集合，
借用记录只增不改状态字段，历史完整保留。
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from .models import (DATA_ITEM_STATUSES, LOAN_STATUSES, OVERDUE_ACTIONS,
                     new_id)

# 默认借期：2 小时
DEFAULT_LEASE_MINUTES = 120


class TestDataManager:
    """测试数据池与借用生命周期管理。"""

    def __init__(self, registry, notify_manager=None):
        self._pools = registry.store("data_pools")
        self._items = registry.store("data_items")
        self._loans = registry.store("data_loans")
        self._notify = notify_manager
        # 借用临界区：查询空闲 → 占用 必须串行，否则两个并发请求
        # 会同时看到同一条 available 数据而重复借出。
        self._borrow_lock = threading.Lock()

    # ------------------------------------------------------------------ 数据池
    def create_pool(self, project_id: str, payload: dict) -> dict:
        name = (payload.get("name") or "").strip()
        if not name:
            return {"error": "数据池名称不能为空"}
        envs = [e.strip() for e in (payload.get("envs") or ["dev"]) if str(e).strip()]
        if not envs:
            envs = ["dev"]
        overdue_action = payload.get("overdue_action", "notify")
        if overdue_action not in OVERDUE_ACTIONS:
            overdue_action = "notify"
        pool = {
            "id": new_id("pool"),
            "project_id": project_id,
            "name": name,
            "category": payload.get("category", "通用"),
            "description": payload.get("description", ""),
            "envs": envs,
            "default_lease_minutes": int(payload.get("default_lease_minutes")
                                         or DEFAULT_LEASE_MINUTES),
            "overdue_action": overdue_action,
            "created_at": time.time(),
        }
        self._pools.insert(pool)
        return pool

    def list_pools(self, project_id: str) -> list[dict]:
        pools = self._pools.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="asc")
        for pool in pools:
            pool["counts"] = self._pool_counts(pool["id"])
        return pools

    def get_pool(self, pool_id: str) -> Optional[dict]:
        return self._pools.get(pool_id)

    def update_pool(self, pool_id: str, patch: dict) -> Optional[dict]:
        allowed = ("name", "category", "description", "envs",
                   "default_lease_minutes", "overdue_action")
        clean = {k: v for k, v in patch.items() if k in allowed}
        if "overdue_action" in clean and clean["overdue_action"] not in OVERDUE_ACTIONS:
            clean["overdue_action"] = "notify"
        if "envs" in clean:
            envs = [e for e in (clean["envs"] or []) if str(e).strip()]
            if not envs:
                clean.pop("envs")  # 不允许把环境列表清空，忽略本次修改
            else:
                clean["envs"] = envs
        return self._pools.update(pool_id, clean)

    def delete_pool(self, pool_id: str) -> dict:
        """删除数据池。池内还有数据或存在未归还借用时拒绝删除。"""
        items = self._items.query(where=[("pool_id", "eq", pool_id)], limit=1)
        if items:
            return {"error": "数据池内还有数据，请先清空后再删除"}
        return {"ok": True, "deleted": self._pools.delete(pool_id)}

    def _pool_counts(self, pool_id: str) -> dict:
        counts = {s: 0 for s in DATA_ITEM_STATUSES}
        counts["total"] = 0
        counts["overdue"] = 0
        now_ts = time.time()
        for item in self._items.query(where=[("pool_id", "eq", pool_id)]):
            counts["total"] += 1
            status = item.get("status", "available")
            counts[status] = counts.get(status, 0) + 1
            if status == "in_use":
                loan = self._loans.get(item.get("current_loan_id") or "")
                if loan and loan.get("due_at") and loan["due_at"] < now_ts:
                    counts["overdue"] += 1
        return counts

    # ------------------------------------------------------------------ 数据项
    def add_item(self, pool_id: str, payload: dict) -> dict:
        pool = self._pools.get(pool_id)
        if pool is None:
            return {"error": "数据池不存在"}
        key = (payload.get("key") or "").strip()
        if not key:
            return {"error": "数据标识 key 不能为空"}
        env = payload.get("env") or (pool.get("envs") or ["dev"])[0]
        if env not in (pool.get("envs") or []):
            return {"error": f"环境 {env} 不在数据池允许的环境列表 {pool.get('envs')} 内"}
        dup = self._find_by_key(pool_id, env, key)
        if dup:
            return {"error": f"数据 {key} 在环境 {env} 下已存在", "item": dup}
        item = {
            "id": new_id("td"),
            "project_id": pool["project_id"],
            "pool_id": pool_id,
            "env": env,
            "key": key,
            "value": payload.get("value") if payload.get("value") is not None else {},
            "tags": payload.get("tags") or [],
            "status": "available",
            "current_loan_id": None,
            "borrowed_by": None,
            "borrowed_at": None,
            "borrow_count": 0,
            "created_at": time.time(),
        }
        self._items.insert(item)
        return item

    def import_items(self, pool_id: str, entries: list[dict],
                     env: Optional[str] = None,
                     tags: Optional[list] = None) -> dict:
        """批量导入。同池同环境按 key 去重：已存在的跳过，返回导入明细。"""
        pool = self._pools.get(pool_id)
        if pool is None:
            return {"error": "数据池不存在"}
        imported, skipped, failed = [], [], []
        for entry in entries:
            if not isinstance(entry, dict):
                entry = {"key": str(entry)}
            merged = dict(entry)
            merged.setdefault("env", env)
            merged["tags"] = list(tags or []) + list(entry.get("tags") or [])
            result = self.add_item(pool_id, merged)
            if "error" in result:
                if result.get("item"):  # 重复 key：跳过而非失败
                    skipped.append({"key": merged.get("key"), "reason": result["error"]})
                else:
                    failed.append({"key": merged.get("key"), "reason": result["error"]})
            else:
                imported.append(result)
        return {"imported": len(imported), "skipped": len(skipped),
                "failed": len(failed), "items": imported,
                "skipped_detail": skipped, "failed_detail": failed}

    def _find_by_key(self, pool_id: str, env: str, key: str) -> Optional[dict]:
        rows = self._items.query(where=[("pool_id", "eq", pool_id),
                                        ("env", "eq", env),
                                        ("key", "eq", key)], limit=1)
        return rows[0] if rows else None

    def get_item(self, item_id: str) -> Optional[dict]:
        return self._items.get(item_id)

    def update_item(self, item_id: str, patch: dict) -> Optional[dict]:
        allowed = ("key", "value", "tags", "status")
        clean = {k: v for k, v in patch.items() if k in allowed}
        if "status" in clean:
            if clean["status"] not in DATA_ITEM_STATUSES:
                clean["status"] = "available"
            # 借出中的数据不允许直接改状态，必须先归还或回收
            item = self._items.get(item_id)
            if item and item.get("status") == "in_use" and clean["status"] != "in_use":
                return {"error": "数据借出中，请先归还或强制回收"}
        return self._items.update(item_id, clean)

    def delete_item(self, item_id: str) -> dict:
        item = self._items.get(item_id)
        if item is None:
            return {"error": "数据不存在"}
        if item.get("status") == "in_use":
            return {"error": "数据借出中，请先归还或强制回收后再删除"}
        return {"ok": True, "deleted": self._items.delete(item_id)}

    def list_items(self, project_id: str, pool_id: str = None,
                   env: str = None, status: str = None,
                   tag: str = None, q: str = None) -> list[dict]:
        where = [("project_id", "eq", project_id)]
        if pool_id:
            where.append(("pool_id", "eq", pool_id))
        if env:
            where.append(("env", "eq", env))
        if status:
            where.append(("status", "eq", status))
        if tag:
            where.append(("tags", "contains", tag))
        items = self._items.query(where=where, order_by="created_at", order="asc")
        if q:
            ql = q.lower()
            items = [it for it in items
                     if ql in str(it.get("key", "")).lower()
                     or ql in str(it.get("value", "")).lower()]
        now_ts = time.time()
        for item in items:
            self._enrich_item(item, now_ts)
        return items

    def _enrich_item(self, item: dict, now_ts: float = None) -> dict:
        """给数据项附上当前借用信息（借用人 / 已借时长 / 是否过期）。"""
        now_ts = now_ts or time.time()
        item["overdue"] = False
        item["borrowed_seconds"] = None
        if item.get("status") == "in_use" and item.get("current_loan_id"):
            loan = self._loans.get(item["current_loan_id"])
            if loan and loan.get("status") == "active":
                item["borrowed_seconds"] = max(0, now_ts - loan.get("borrowed_at", now_ts))
                item["due_at"] = loan.get("due_at")
                item["overdue"] = bool(loan.get("due_at") and loan["due_at"] < now_ts)
        return item

    # ------------------------------------------------------------------ 借用
    def borrow(self, project_id: str, pool_id: str, borrower: str,
               env: str = None, tags: Optional[list] = None,
               item_id: str = None, lease_minutes: int = None,
               purpose: str = "") -> dict:
        """申请借用一条数据。

        指定 ``item_id`` 则借指定数据；否则按「池 + 环境 + 标签全匹配」
        在空闲数据中自动分配（优先借出次数最少的，均衡磨损）。
        整个「找空闲 → 占用」在进程锁内完成并复核状态，保证并发安全。
        """
        borrower = (borrower or "").strip()
        if not borrower:
            return {"error": "借用人不能为空"}
        pool = self._pools.get(pool_id)
        if pool is None:
            return {"error": "数据池不存在"}
        if pool.get("project_id") != project_id:
            return {"error": "数据池不属于该项目"}

        with self._borrow_lock:
            if item_id:
                item = self._items.get(item_id)
                if item is None or item.get("pool_id") != pool_id:
                    return {"error": "数据不存在或不属于该数据池"}
                # 指定数据时未显式给环境，则按数据自身归属环境借用
                if env is None:
                    env = item.get("env")
                if item.get("env") != env:
                    return {"error": f"数据属于环境 {item.get('env')}，"
                                     f"不能在环境 {env} 借用（环境隔离）"}
                candidates = [item]
            else:
                env = env or (pool.get("envs") or ["dev"])[0]
                if env not in (pool.get("envs") or []):
                    return {"error": f"环境 {env} 不在数据池允许的环境列表"
                                     f" {pool.get('envs')} 内"}
                where = [("pool_id", "eq", pool_id),
                         ("env", "eq", env),
                         ("status", "eq", "available")]
                candidates = self._items.query(where=where,
                                               order_by="borrow_count",
                                               order="asc")
                want_tags = set(tags or [])
                if want_tags:
                    candidates = [c for c in candidates
                                  if want_tags.issubset(set(c.get("tags") or []))]

            # 复核状态：候选可能在我们查询后被另一请求占用
            chosen = None
            for cand in candidates:
                fresh = self._items.get(cand["id"])
                if fresh and fresh.get("status") == "available":
                    chosen = fresh
                    break
            if chosen is None:
                if item_id:
                    return {"error": "数据已被借出或停用，请稍后再试"}
                return {"error": "没有符合条件的空闲数据"
                                 + (f"（标签 {sorted(tags)}）" if tags else "")}

            now_ts = time.time()
            lease = int(lease_minutes or pool.get("default_lease_minutes")
                        or DEFAULT_LEASE_MINUTES)
            loan = {
                "id": new_id("loan"),
                "project_id": project_id,
                "pool_id": pool_id,
                "pool_name": pool.get("name"),
                "item_id": chosen["id"],
                "item_key": chosen.get("key"),
                "env": env,
                "borrower": borrower,
                "purpose": purpose or "",
                "status": "active",
                "borrowed_at": now_ts,
                "due_at": now_ts + lease * 60,
                "lease_minutes": lease,
                "returned_at": None,
                "duration": None,
                "reminded": False,
                "created_at": now_ts,
            }
            self._loans.insert(loan)
            self._items.update(chosen["id"], {
                "status": "in_use",
                "current_loan_id": loan["id"],
                "borrowed_by": borrower,
                "borrowed_at": now_ts,
                "borrow_count": chosen.get("borrow_count", 0) + 1,
            })
        loan["item"] = self._items.get(chosen["id"])
        return loan

    # ------------------------------------------------------------------ 归还 / 回收
    def return_item(self, item_id: str = None, loan_id: str = None,
                    borrower: str = None) -> dict:
        """归还数据：关闭借用记录，数据回到可用状态。"""
        loan = self._find_active_loan(item_id=item_id, loan_id=loan_id)
        if loan is None:
            return {"error": "没有找到借出中的记录（可能已归还或被回收）"}
        if borrower and loan.get("borrower") != borrower:
            return {"error": f"该数据由 {loan.get('borrower')} 借出，"
                             f"仅本人可不填借用人归还"}
        return self._close_loan(loan, "returned")

    def reclaim(self, item_id: str = None, loan_id: str = None,
                operator: str = "admin") -> dict:
        """强制回收：管理员/系统把借出中的数据立即收回重新可用。"""
        loan = self._find_active_loan(item_id=item_id, loan_id=loan_id)
        if loan is None:
            return {"error": "没有找到借出中的记录"}
        result = self._close_loan(loan, "force_reclaimed", operator=operator)
        if "error" not in result:
            self._fire(loan["project_id"], "testdata.reclaimed", {
                "loan_id": loan["id"], "item_id": loan["item_id"],
                "item_key": loan.get("item_key"), "pool_name": loan.get("pool_name"),
                "env": loan.get("env"), "borrower": loan.get("borrower"),
                "operator": operator,
            })
        return result

    def _find_active_loan(self, item_id: str = None,
                          loan_id: str = None) -> Optional[dict]:
        if loan_id:
            loan = self._loans.get(loan_id)
            if loan and loan.get("status") == "active":
                return loan
            return None
        if item_id:
            item = self._items.get(item_id)
            if item and item.get("current_loan_id"):
                loan = self._loans.get(item["current_loan_id"])
                if loan and loan.get("status") == "active":
                    return loan
        return None

    def _close_loan(self, loan: dict, end_status: str,
                    operator: str = None) -> dict:
        if end_status not in LOAN_STATUSES:
            return {"error": f"非法结束状态 {end_status}"}
        with self._borrow_lock:
            # 复核：可能已被并发归还/回收
            fresh = self._loans.get(loan["id"])
            if not fresh or fresh.get("status") != "active":
                return {"error": "该借用已结束"}
            now_ts = time.time()
            patch = {
                "status": end_status,
                "returned_at": now_ts,
                "duration": max(0, now_ts - fresh.get("borrowed_at", now_ts)),
            }
            if operator:
                patch["reclaimed_by"] = operator
            updated_loan = self._loans.update(fresh["id"], patch)
            self._items.update(fresh["item_id"], {
                "status": "available",
                "current_loan_id": None,
                "borrowed_by": None,
                "borrowed_at": None,
            })
        return updated_loan

    # ------------------------------------------------------------------ 历史 / 列表
    def item_history(self, item_id: str) -> list[dict]:
        """一条数据的完整借用历史（新的在前）。"""
        return self._loans.query(where=[("item_id", "eq", item_id)],
                                 order_by="borrowed_at", order="desc")

    def list_loans(self, project_id: str, status: str = None,
                   borrower: str = None, pool_id: str = None,
                   env: str = None, limit: int = 200) -> list[dict]:
        where = [("project_id", "eq", project_id)]
        if status:
            where.append(("status", "eq", status))
        if borrower:
            where.append(("borrower", "eq", borrower))
        if pool_id:
            where.append(("pool_id", "eq", pool_id))
        if env:
            where.append(("env", "eq", env))
        loans = self._loans.query(where=where, order_by="borrowed_at",
                                  order="desc", limit=limit)
        now_ts = time.time()
        for loan in loans:
            if loan.get("status") == "active":
                loan["borrowed_seconds"] = max(0, now_ts - loan.get("borrowed_at", now_ts))
                loan["overdue"] = bool(loan.get("due_at") and loan["due_at"] < now_ts)
            else:
                loan["borrowed_seconds"] = loan.get("duration")
                loan["overdue"] = False
        return loans

    # ------------------------------------------------------------------ 过期扫描
    def check_overdue(self, now_ts: float = None) -> dict:
        """扫描所有过期未归还的借用，按池策略提醒或强制回收。

        - 池策略 ``notify``：给借用人发一次过期提醒（通知事件），数据仍锁定；
        - 池策略 ``reclaim``：强制回收，数据立即重新可用，并发回收通知。

        返回 ``{"reminded": n, "reclaimed": n}``，供调度周期任务与手动触发。
        """
        now_ts = now_ts or time.time()
        reminded = reclaimed = 0
        active = self._loans.query(where=[("status", "eq", "active")])
        for loan in active:
            due = loan.get("due_at")
            if not due or due >= now_ts:
                continue
            pool = self._pools.get(loan.get("pool_id")) or {}
            overdue_minutes = round((now_ts - due) / 60, 1)
            payload = {
                "loan_id": loan["id"], "item_id": loan["item_id"],
                "item_key": loan.get("item_key"), "pool_name": loan.get("pool_name"),
                "env": loan.get("env"), "borrower": loan.get("borrower"),
                "overdue_minutes": overdue_minutes,
            }
            if pool.get("overdue_action") == "reclaim":
                result = self._close_loan(loan, "force_reclaimed",
                                          operator="system(过期自动回收)")
                if "error" not in result:
                    reclaimed += 1
                    self._fire(loan["project_id"], "testdata.reclaimed", payload)
            elif not loan.get("reminded"):
                # 只提醒一次，避免每个扫描周期重复轰炸
                self._loans.update(loan["id"], {"reminded": True})
                reminded += 1
                self._fire(loan["project_id"], "testdata.overdue", payload)
        return {"reminded": reminded, "reclaimed": reclaimed}

    def _fire(self, project_id: str, event: str, payload: dict) -> None:
        if self._notify is None or not project_id:
            return
        try:
            self._notify.fire(project_id, event, payload)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ 统计
    def stats(self, project_id: str) -> dict:
        items = self._items.query(where=[("project_id", "eq", project_id)])
        now_ts = time.time()
        by_status = {s: 0 for s in DATA_ITEM_STATUSES}
        by_env: dict[str, int] = {}
        overdue = 0
        for item in items:
            status = item.get("status", "available")
            by_status[status] = by_status.get(status, 0) + 1
            env = item.get("env", "dev")
            by_env[env] = by_env.get(env, 0) + 1
            if status == "in_use":
                loan = self._loans.get(item.get("current_loan_id") or "")
                if loan and loan.get("due_at") and loan["due_at"] < now_ts:
                    overdue += 1
        pools = self._pools.query(where=[("project_id", "eq", project_id)])
        active_loans = self._loans.query(where=[("project_id", "eq", project_id),
                                                ("status", "eq", "active")])
        return {
            "total": len(items),
            "pools": len(pools),
            "by_status": by_status,
            "by_env": by_env,
            "active_loans": len(active_loans),
            "overdue": overdue,
        }
