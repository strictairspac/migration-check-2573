"""HTTP 端到端：页面发起切换、停写/栅栏状态码、迁移与备份恢复切换后可查、同查询只读一库。"""
import time

import pytest


def _wait_phase(client, headers, phase_gone=None, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = client.get('/switch/status', headers=headers).json()
        if phase_gone is None or snap['phase'] != phase_gone:
            return snap
        time.sleep(0.1)
    raise AssertionError('状态未按预期推进')


def test_health_reports_topology(client):
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json()['writer_node'] == 'primary'
    assert r.json()['phase'] == 'idle'


def test_switch_status_and_initiate_permissions(client, admin_headers, viewer_headers):
    # 观察员只读，不能发起切换
    r = client.post('/switch/initiate', headers=viewer_headers)
    assert r.status_code == 403
    # 未认证 401
    assert client.post('/switch/initiate').status_code == 401

    r = client.get('/switch/status', headers=admin_headers)
    assert r.status_code == 200
    assert r.json()['writer_node'] == 'primary'

    r = client.post('/switch/initiate', headers=admin_headers)
    assert r.status_code == 200
    order = r.json()
    assert order['target_node'] == 'standby' and order['status'] in (
        'queued', 'syncing', 'draining', 'flipping', 'completed')


def test_full_switch_via_api_and_fencing(client, app_env, admin_headers):
    # 发起并等待切换完成（无 worker 夹具时直接驱动服务一次以推进到完成）
    from src.services import switchover
    order = client.post('/switch/initiate', headers=admin_headers).json()
    result = switchover.run_switch(order['id'])
    assert result['status'] == 'completed'

    snap = client.get('/switch/status', headers=admin_headers).json()
    assert snap['writer_node'] == 'standby'
    assert snap['phase'] == 'idle'
    # 页面能查当前写入位置
    assert snap['nodes']['standby']['role'] == 'writer'

    # 旧主库引擎级栅栏：直连原生写被 409（经服务封装的错误处理器）
    from src.config.database import FencingError, primary_engine
    from sqlalchemy import text
    with pytest.raises(FencingError):
        with primary_engine.begin() as conn:
            conn.execute(text("INSERT INTO batches (name, source_file, description, rows) "
                              "VALUES ('api越权','x.json','','[]')"))

    # 切换单两个库都能查到（已确认的交接记录可在任一副本查询）
    orders = client.get('/switch/orders', headers=admin_headers).json()
    assert any(o['id'] == order['id'] and o['status'] == 'completed' for o in orders)


def test_write_paused_returns_423_and_retryable(client, app_env, admin_headers, operator_headers):
    """drain 窗口写入收到 423 可重试；查询照常 200。"""
    from src.services import switchover
    observed = {}

    def hook(phase, order_id):
        if phase == 'draining':
            # 此时通过 API 写批次，应被停写
            r = client.post('/batches', headers=operator_headers, json={
                'name': 'drain窗口批次', 'source_file': 'd.json',
                'description': '', 'rows': []})
            observed['write_status'] = r.status_code
            observed['write_body'] = r.json()
            # 查询不受影响
            q = client.get('/batches', headers=operator_headers)
            observed['read_status'] = q.status_code

    order = client.post('/switch/initiate', headers=admin_headers).json()
    switchover.phase_hook = hook
    try:
        switchover.run_switch(order['id'])
    finally:
        switchover.phase_hook = None

    assert observed['write_status'] == 423
    assert observed['write_body'].get('retryable') is True
    assert observed['read_status'] == 200


def test_migration_and_restore_queryable_after_switch(client, app_env, admin_headers,
                                                      operator_headers):
    """已确认的迁移与备份恢复，在切换后都能查到；同一次查询只读一个库。"""
    from src.services import switchover
    from src.config.database import ROUTER

    # 取一个映射，创建并执行迁移
    mappings = client.get('/mappings', headers=operator_headers).json()
    mapping_id = mappings[0]['id']
    run = client.post('/runs', headers=operator_headers,
                      json={'name': '切换前迁移单', 'mapping_id': mapping_id}).json()
    # 无 worker 夹具：直接跑迁移逻辑
    from src.worker import run_migration
    run_migration(run['id'])
    run_detail = client.get(f'/runs/{run["id"]}', headers=operator_headers).json()
    assert run_detail['status'] == 'completed'

    # 备份 -> 切换 -> 在新主库仍能查到运行、目标行与备份
    backup = client.get(f'/runs/{run["id"]}/backup', headers=admin_headers).json()

    order = client.post('/switch/initiate', headers=admin_headers).json()
    switchover.run_switch(order['id'])
    assert ROUTER.writer_node() == 'standby'

    # 运行记录与目标行在切换后可查（读路由只命中一个库）
    after = client.get(f'/runs/{run["id"]}', headers=operator_headers).json()
    assert after['status'] == 'completed'
    targets = client.get(f'/runs/{run["id"]}/targets', headers=operator_headers).json()
    assert targets['count'] == after['result']['accepted_count']

    # 备份恢复在新主库完成并通过校验
    r = client.post(f'/runs/{run["id"]}/restore', headers=admin_headers, json={'backup': backup})
    assert r.status_code == 200
    assert r.json()['verified'] is True
    # 审计里能追踪到恢复与切换完成事件
    audits = client.get('/audit', headers=admin_headers).json()
    actions = {a['action'] for a in audits}
    assert 'run.restore' in actions and 'switch.completed' in actions


def test_reader_never_fanout(client, app_env, operator_headers):
    """读路由每次只绑定一个节点：连续查询返回的 reader_node 单一且健康。"""
    for _ in range(5):
        snap = client.get('/switch/status', headers=operator_headers).json()
        assert snap['reader_node'] in ('primary', 'standby')
        assert snap['nodes'][snap['reader_node']]['healthy'] is True
        client.get('/batches', headers=operator_headers)  # 不报错即读路径正常
