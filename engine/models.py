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

# -- 测试数据管理（TDM） ----------------------------------------------------

# 数据池支持的逻辑环境（多环境隔离的环境轴；与具体项目的环境实例解耦）
TDM_ENVIRONMENTS = ["dev", "staging", "prod"]

# 数据项 / 借用单状态
TDM_DATA_STATUSES = ["available", "borrowed", "reserved", "retired"]
TDM_LEASE_STATUSES = ["active", "returned", "overdue", "reclaimed"]

# 过期策略：仅提醒 / 提醒后强制回收
TDM_OVERDUE_POLICIES = ["remind", "reclaim"]


def new_id(prefix: str) -> str:
    """生成带前缀的唯一 id（时间戳 + 随机后缀，便于阅读与排查）。"""
    return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"


def now() -> float:
    return time.time()
