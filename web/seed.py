"""演示 / 初始数据生成。

应用启动时，若数据目录里还没有任何项目，会自动调用 :func:`seed_demo_data`
生成一份演示数据（项目 + 用例 + 套件 + 环境 + 计划 + 集成），让各个页面
一打开就有内容可点、可测。HTTP 接口 ``POST /api/seed/demo`` 也复用这里，
供前端「生成演示项目」按钮调用。
"""

from __future__ import annotations

import time

from engine import new_id


def seed_demo_data(registry, env_mgr, notify_mgr, testdata_mgr=None) -> dict:
    """生成演示项目，返回 ``{"project": ..., "env_id": ..., "suite_id": ...}``。"""
    proj = {
        "id": new_id("proj"),
        "name": "演示项目 · 测试与CI",
        "description": "内置示例用例、套件、环境与通知集成的演示项目。",
        "repo_url": "https://example.com/demo",
        "auto_create_defects": True,
        "created_at": time.time(),
    }
    registry.store("projects").insert(proj)
    pid = proj["id"]

    env = env_mgr.create(pid, {
        "name": "dev 开发环境",
        "python_version": "3.11",
        "base_image": "python:3.11-slim",
        "variables": {"BASE_URL": "http://dev.mock.local", "REGION": "dev"},
        "config": {"base_url": "http://dev.mock.local", "latency_ms": 15, "fail_rate": 0.0},
        "dependencies": [
            {"name": "requests", "constraint": ">=2.28"},
            {"name": "pytest", "constraint": ">=7.0"},
            {"name": "flask", "constraint": ">=3.0"},
        ],
    })
    env2 = env_mgr.create(pid, {
        "name": "staging 预发环境",
        "python_version": "3.12",
        "base_image": "python:3.12-slim",
        "variables": {"BASE_URL": "http://staging.mock.local", "REGION": "staging"},
        "config": {"base_url": "http://staging.mock.local", "latency_ms": 45, "fail_rate": 0.15},
        "dependencies": [
            {"name": "requests", "constraint": ">=2.30"},
            {"name": "django", "constraint": ">=4.2"},
            {"name": "numpy", "constraint": ">=1.24"},
        ],
    })

    def _case(name, priority, tags, steps):
        return registry.store("cases").insert({
            "id": new_id("case"),
            "project_id": pid,
            "name": name,
            "description": "演示用例",
            "priority": priority,
            "tags": tags,
            "timeout": 60,
            "enabled": True,
            "steps": steps,
            "created_at": time.time(),
        })

    c1 = _case("健康检查接口", "P0", ["smoke", "api"], [
        {"action": "request", "method": "GET", "url": "/api/health", "name": "请求健康检查"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
        {"action": "assert", "type": "truthy", "actual": "${resp.body.ok}", "expected": True, "name": "返回 ok"},
    ])
    c2 = _case("登录接口", "P0", ["smoke", "auth"], [
        {"action": "set", "key": "user", "value": "admin", "name": "准备用户名"},
        {"action": "request", "method": "POST", "url": "/api/login", "name": "请求登录"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "登录成功"},
        {"action": "assert", "type": "contains", "actual": "${resp.body}", "expected": "ok", "name": "返回体含 ok"},
    ])
    c3 = _case("用户列表查询", "P1", ["api", "users"], [
        {"action": "request", "method": "GET", "url": "/api/users", "name": "查询用户列表"},
        {"action": "script", "expr": "len([1,2,3])", "save_as": "count", "name": "计算数量"},
        {"action": "assert", "type": "gte", "actual": "${count}", "expected": 3, "name": "数量 >= 3"},
    ])
    c4 = _case("创建项目", "P1", ["api", "projects"], [
        {"action": "request", "method": "POST", "url": "/api/projects", "name": "创建项目"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
    ])
    c5 = _case("慢接口（性能）", "P2", ["perf"], [
        {"action": "request", "method": "GET", "url": "/api/slow", "name": "请求慢接口"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "状态码 200"},
    ])
    c6 = _case("失败注入接口", "P2", ["chaos"], [
        {"action": "request", "method": "GET", "url": "/api/error", "name": "请求失败接口"},
        {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200, "name": "期望 200"},
    ])
    c7 = _case("字符串断言", "P2", ["unit"], [
        {"action": "script", "expr": "2 + 3 * 4", "save_as": "result", "name": "算术"},
        {"action": "assert", "type": "equals", "actual": "${result}", "expected": 14, "name": "结果等于 14"},
        {"action": "assert", "type": "between", "actual": "${result}", "expected": [10, 20], "name": "结果在 10~20"},
    ])
    c8 = _case("正则断言", "P3", ["unit"], [
        {"action": "set", "key": "text", "value": "release-2.31.0", "name": "设置文本"},
        {"action": "assert", "type": "regex", "actual": "${text}", "expected": r"^\d+\.\d+", "name": "匹配版本号"},
    ])

    suite = {
        "id": new_id("suite"),
        "project_id": pid,
        "name": "冒烟测试套件",
        "description": "核心链路冒烟",
        "group": "smoke",
        "env_id": env["id"],
        "case_ids": [c1, c2, c3, c4, c5, c6, c7, c8],
        "created_at": time.time(),
    }
    registry.store("suites").insert(suite)

    registry.store("schedules").insert({
        "id": new_id("sch"),
        "project_id": pid,
        "name": "每 10 分钟跑一次冒烟",
        "cron": "*/10 * * * *",
        "suite_id": suite["id"],
        "env_id": env["id"],
        "enabled": False,
        "last_fired_minute": None,
        "created_at": time.time(),
    })

    notify_mgr.create(pid, {
        "type": "webhook",
        "name": "CI Webhook",
        "config": {"url": "https://example.com/hooks/ci"},
        "events": ["build.finished", "build.failed",
                   "testdata.overdue", "testdata.reclaimed"],
    })
    notify_mgr.create(pid, {
        "type": "email",
        "name": "团队邮件",
        "config": {"address": "qa@example.com"},
        "events": ["build.failed", "testdata.overdue"],
    })

    if testdata_mgr is not None:
        _seed_testdata(registry, testdata_mgr, pid)

    return {"project": proj, "env_id": env["id"], "suite_id": suite["id"]}


def _seed_testdata(registry, mgr, pid: str) -> None:
    """演示测试数据：账号池（dev/staging 隔离）+ 订单号池，含借出中与过期数据。"""
    acc_pool = mgr.create_pool(pid, {
        "name": "测试账号池",
        "category": "账号",
        "description": "登录/鉴权用例共用的测试账号，按环境隔离，借期 2 小时。",
        "envs": ["dev", "staging"],
        "default_lease_minutes": 120,
        "overdue_action": "notify",
    })
    order_pool = mgr.create_pool(pid, {
        "name": "订单号池",
        "category": "订单",
        "description": "一次性订单号，借期 30 分钟，过期自动回收复用。",
        "envs": ["dev", "staging"],
        "default_lease_minutes": 30,
        "overdue_action": "reclaim",
    })

    mgr.import_items(acc_pool["id"], [
        {"key": "qa_admin", "value": {"username": "qa_admin", "password": "Aa123456",
                                      "role": "admin"}, "tags": ["登录", "管理员"]},
        {"key": "qa_user01", "value": {"username": "qa_user01", "password": "Aa123456",
                                       "role": "user"}, "tags": ["登录"]},
        {"key": "qa_user02", "value": {"username": "qa_user02", "password": "Aa123456",
                                       "role": "user"}, "tags": ["登录", "支付"]},
        {"key": "qa_vip", "value": {"username": "qa_vip", "password": "Aa123456",
                                    "role": "vip"}, "tags": ["会员", "支付"]},
    ], env="dev")
    mgr.import_items(acc_pool["id"], [
        {"key": "stg_admin", "value": {"username": "stg_admin", "password": "Ss123456",
                                       "role": "admin"}, "tags": ["登录", "管理员"]},
        {"key": "stg_user01", "value": {"username": "stg_user01", "password": "Ss123456",
                                        "role": "user"}, "tags": ["登录"]},
    ], env="staging")
    mgr.import_items(order_pool["id"], [
        {"key": f"ORD-DEV-{10001 + i}", "value": {"order_no": f"ORDDEV{10001 + i}"},
         "tags": ["一次性"]}
        for i in range(6)
    ], env="dev")
    mgr.import_items(order_pool["id"], [
        {"key": f"ORD-STG-{20001 + i}", "value": {"order_no": f"ORDSTG{20001 + i}"},
         "tags": ["一次性"]}
        for i in range(3)
    ], env="staging")

    # 制造演示现场，三种状态各一：
    # 1) zhangsan 借出中未过期；2) lisi 过期未还（notify 池 → 保持锁定、提醒）；
    # 3) wangwu 过期（reclaim 池 → 启动扫描时自动回收，产生回收通知事件）。
    import time as _time
    loans_store = registry.store("data_loans")
    now_ts = _time.time()
    loan = mgr.borrow(pid, acc_pool["id"], "zhangsan", env="dev",
                      tags=["登录"], purpose="登录接口联调")
    if "id" in loan:
        loans_store.update(loan["id"],
                           {"borrowed_at": now_ts - 20 * 60})
    overdue_loan = mgr.borrow(pid, acc_pool["id"], "lisi", env="dev",
                              tags=["登录"], purpose="支付链路回归")
    if "id" in overdue_loan:
        past = now_ts - 3 * 3600  # 3 小时前借出，借期 2 小时 → 已过期
        loans_store.update(overdue_loan["id"],
                           {"borrowed_at": past, "due_at": past + 120 * 60})
    reclaim_loan = mgr.borrow(pid, order_pool["id"], "wangwu", env="dev",
                              purpose="下单链路回归")
    if "id" in reclaim_loan:
        past = now_ts - 45 * 60  # 45 分钟前借出，借期 30 分钟 → 已过期
        loans_store.update(reclaim_loan["id"],
                           {"borrowed_at": past, "due_at": past + 30 * 60})
