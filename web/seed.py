"""演示 / 初始数据生成。

应用启动时，若数据目录里还没有任何项目，会自动调用 :func:`seed_demo_data`
生成一份演示数据（项目 + 用例 + 套件 + 环境 + 计划 + 集成），让各个页面
一打开就有内容可点、可测。HTTP 接口 ``POST /api/seed/demo`` 也复用这里，
供前端「生成演示项目」按钮调用。
"""

from __future__ import annotations

import time

from engine import new_id


def seed_demo_data(registry, env_mgr, notify_mgr, tdm_mgr=None) -> dict:
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
        "events": ["build.finished", "build.failed"],
    })
    notify_mgr.create(pid, {
        "type": "email",
        "name": "团队邮件",
        "config": {"address": "qa@example.com"},
        "events": ["build.failed"],
    })

    # -- 测试数据池：账号池（提醒）+ 订单号池（宽限期满强制回收） -----------
    if tdm_mgr is not None:
        account_pool = tdm_mgr.create_pool(pid, {
            "name": "测试账号池",
            "category": "账号",
            "description": "登录账号，借走即锁定，用后归还。",
            "envs": ["dev", "staging"],
            "default_ttl": 4 * 3600,
            "overdue_policy": "remind",
            "tags": ["account", "auth"],
        })
        tdm_mgr.import_items(account_pool["id"], [
            {"env": "dev", "data_key": f"user_{n:03d}@qa.local",
             "payload": {"username": f"user_{n:03d}", "password": "Pa$$w0rd",
                         "level": "vip" if n % 3 == 0 else "normal"},
             "tags": ["vip"] if n % 3 == 0 else ["normal"]}
            for n in range(1, 7)
        ] + [
            {"env": "staging", "data_key": "user_001@qa.local",
             "payload": {"username": "user_001", "password": "Staging#1",
                         "level": "admin"},
             "tags": ["admin"]},
            {"env": "staging", "data_key": "user_002@qa.local",
             "payload": {"username": "user_002", "password": "Staging#2",
                         "level": "normal"},
             "tags": ["normal"]},
        ])

        order_pool = tdm_mgr.create_pool(pid, {
            "name": "订单号池",
            "category": "订单",
            "description": "可复用的测试订单号，替代写死在用例里的常量。",
            "envs": ["dev", "staging"],
            "default_ttl": 3600,
            "overdue_policy": "reclaim",
            "grace_seconds": 1800,
            "tags": ["order"],
        })
        tdm_mgr.import_items(order_pool["id"], [
            {"env": "dev", "data_key": f"ORD-2026{n:06d}",
             "payload": {"sku": f"SKU-{1000 + n}", "amount": n * 37},
             "tags": ["paid" if n % 2 else "unpaid"]}
            for n in range(1, 9)
        ])

        # 借一条 dev 账号给「张三」（正常在用）
        dev_items = tdm_mgr.list_items(pool_id=account_pool["id"], env="dev")
        tdm_mgr.borrow_item(dev_items[0]["id"], "张三",
                            ttl=4 * 3600, purpose="登录链路回归",
                            source="case:case_login_smoke")
        # 借一条 staging 账号给「李四」并手动置为过期，用于演示过期提醒
        st_items = tdm_mgr.list_items(pool_id=account_pool["id"], env="staging")
        overdue_lease = tdm_mgr.borrow_item(
            st_items[0]["id"], "李四", ttl=3600, purpose="权限校验专项")
        past = time.time() - 5400  # 1.5 小时前借出、应还时间已过
        registry.store("tdm_leases").update(overdue_lease["id"], {
            "borrowed_at": past, "due_at": past + 3600,
        })
        # 借一个订单号给「自动化任务·nightly」（订单池，用于演示强制回收）
        tdm_mgr.borrow(order_pool["id"], "nightly-bot", "dev", count=1,
                       purpose="夜间下单链路", source="ci")

    return {"project": proj, "env_id": env["id"], "suite_id": suite["id"]}
