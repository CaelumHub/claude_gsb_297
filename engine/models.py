"""领域模型：枚举常量、id 生成与通用工具。"""

from __future__ import annotations

import time
import uuid

# 用例优先级
PRIORITIES = ["P0", "P1", "P2", "P3"]

# 用例结果状态（单条）
CASE_STATUSES = ["passed", "failed", "error", "skipped", "timeout"]

# 构建状态（一次执行）
BUILD_STATUSES = ["pending", "running", "passed", "failed", "cancelled", "error"]

# 缺陷严重级别与状态流
SEVERITIES = ["blocker", "critical", "major", "minor", "trivial"]
DEFECT_STATUSES = ["open", "in_progress", "fixed", "verified", "closed", "reopened"]

# 通知集成类型
INTEGRATION_TYPES = ["webhook", "slack", "email", "dingtalk"]

# 触发来源
TRIGGER_TYPES = ["manual", "schedule", "webhook", "ci"]

# 测试数据状态：available 可借用 / in_use 借出锁定中 / disabled 停用维护
DATA_ITEM_STATUSES = ["available", "in_use", "disabled"]

# 借用记录状态：active 借出中 / returned 已归还 / force_reclaimed 被强制回收
LOAN_STATUSES = ["active", "returned", "force_reclaimed"]

# 数据池过期策略：notify 仅提醒 / reclaim 提醒并强制回收
OVERDUE_ACTIONS = ["notify", "reclaim"]


def new_id(prefix: str) -> str:
    """生成带前缀的唯一 id（时间戳 + 随机后缀，便于阅读与排查）。"""
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"


def now() -> float:
    return time.time()
