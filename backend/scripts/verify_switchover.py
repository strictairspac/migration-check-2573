"""端到端验收脚本：单进程内用 TestClient 驱动完整主副库切换。

单进程保证协调状态（Redis/fakeredis）、双库引擎与 HTTP 层共享同一份 ROUTER，
从而能同时验证 HTTP 契约、读路由、引擎级写栅栏与复制回查。

运行（在 backend 目录）：venv/bin/python scripts/verify_switchover.py
"""
import os
import sys
import pathlib
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ.pop('REDIS_URL', None)  # 本地用进程内 fakeredis，保证状态共享

from starlette.testclient import TestClient  # noqa: E402

from src.config.database import (  # noqa: E402
    ROUTER, FACTORIES, ENGINES, Base, set_node_outage, FencingError,
)
from src.config import queue_client  # noqa: E402
import fakeredis  # noqa: E402
from src.models.batch import Batch  # noqa: E402
from src.services import switchover  # noqa: E402
from src.main import app  # noqa: E402
from sqlalchemy import text  # noqa: E402

PASS = FAIL = 0


def check(name, cond, detail=''):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f'  ✓ {name}')
    else:
        FAIL += 1
        print(f'  ✗ {name} {detail}')


def reset_world():
    queue_client.queue.flushall()
    queue_client.queue.set(ROUTER.KEY_WRITER, 'primary')
    queue_client.queue.set(ROUTER.KEY_TERM, '1')
    queue_client.queue.set(ROUTER.KEY_PHASE, 'idle')
    queue_client.queue.set(ROUTER.KEY_LSN, '0')
    for eng in ENGINES.values():
        eng.dispose()
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng)
    ROUTER._bootstrapped = True
    for n in ('primary', 'standby'):
        set_node_outage(n, False)
    switchover.phase_hook = None
    from scripts.seed_demo import seed
    seed()


def wait_order(c, h, oid, timeout=20):
    dl = time.time() + timeout
    while time.time() < dl:
        for o in c.get('/switch/orders', headers=h).json():
            if o['id'] == oid and o['status'] == 'completed':
                return o
        time.sleep(0.1)
    raise TimeoutError(f'切换单 {oid} 未完成')


def wait_run(c, h, rid, timeout=20):
    dl = time.time() + timeout
    while time.time() < dl:
        d = c.get(f'/runs/{rid}', headers=h).json()
        if d['status'] in ('completed', 'failed', 'cancelled'):
            return d
        time.sleep(0.1)
    raise TimeoutError('迁移未完成')


def main():
    reset_world()
    c = TestClient(app)
    h = {'Authorization': 'Bearer ' + c.post('/auth/login', json={
        'email': 'admin@migration.local', 'password': 'Migrate2026!'}).json()['token']}
    oh = {'Authorization': 'Bearer ' + c.post('/auth/login', json={
        'email': 'operator@migration.local', 'password': 'Migrate2026!'}).json()['token']}

    print('1) 初始拓扑：写主库、双节点健康、同查询只读一库')
    s = c.get('/switch/status', headers=h).json()
    check('初始写库 primary', s['writer_node'] == 'primary')
    check('双节点健康', all(n['healthy'] for n in s['nodes'].values()))
    check('读路由单一节点', s['reader_node'] in ('primary', 'standby'))

    print('2) 迁移完成并下载备份（切换前已确认的数据）')
    mid = c.get('/mappings', headers=oh).json()[0]['id']
    run = c.post('/runs', headers=oh, json={'name': 'e2e迁移', 'mapping_id': mid}).json()
    from src.worker import run_migration
    run_migration(run['id'])  # 单进程无独立 worker，直接驱动
    wait_run(c, oh, run['id'])
    rd = c.get(f'/runs/{run["id"]}', headers=oh).json()
    check('迁移完成', rd['status'] == 'completed')
    backup = c.get(f'/runs/{run["id"]}/backup', headers=h).json()
    accepted = rd['result']['accepted_count']
    check('备份带 checksum', bool(backup['checksum']))

    print('3) 页面发起切换，副库追平前照常接单、交接短暂停写、查询照常')
    paused = {}

    def hook(phase, _oid):
        if phase == 'draining':
            r = c.post('/batches', headers=oh, json={
                'name': 'drain写入', 'source_file': 'd.json', 'description': '', 'rows': []})
            paused['write'] = r.status_code
            paused['read'] = c.get('/batches', headers=oh).status_code
    switchover.phase_hook = hook
    order = c.post('/switch/initiate', headers=h).json()
    switchover.run_switch(order['id'])
    switchover.phase_hook = None
    check('drain 期写入收到 423 停写', paused.get('write') == 423, paused)
    check('drain 期查询照常 200', paused.get('read') == 200)
    wait_order(c, h, order['id'])
    s = c.get('/switch/status', headers=h).json()
    check('写库切到 standby', s['writer_node'] == 'standby')
    check('任期升至 2', s['term'] == 2)
    check('阶段恢复 idle', s['phase'] == 'idle')

    print('4) 已确认的迁移与备份恢复切换后都能查到')
    rd2 = c.get(f'/runs/{run["id"]}', headers=oh).json()
    check('运行记录可查且完成', rd2['status'] == 'completed')
    tg = c.get(f'/runs/{run["id"]}/targets', headers=oh).json()
    check('目标行可查且行数一致', tg['count'] == accepted)
    rr = c.post(f'/runs/{run["id"]}/restore', headers=h, json={'backup': backup}).json()
    check('备份恢复校验通过', rr.get('verified') is True)

    print('5) 旧库重新上线也不能接写（引擎级栅栏）')
    set_node_outage('primary', True)
    check('旧库下线判定为不健康', ROUTER.node_healthy('primary') is False)
    set_node_outage('primary', False)
    check('旧库恢复健康但不抢主', ROUTER.node_healthy('primary') is True
          and ROUTER.writer_node() == 'standby')
    fenced = False
    try:
        with ENGINES['primary'].begin() as conn:
            conn.execute(text("INSERT INTO batches (name, source_file, description, rows) "
                              "VALUES ('越权','x.json','','[]')"))
    except FencingError:
        fenced = True
    check('旧主库原生写被连接层栅栏拒绝', fenced)

    print('6) 新主库写入正常，复制回旧库后两边可查')
    nb = c.post('/batches', headers=oh, json={
        'name': '新主库批次', 'source_file': 'n.json', 'description': '', 'rows': []})
    check('新主库写入成功', nb.status_code in (200, 201), nb.text)
    ROUTER.catch_up('primary')
    with FACTORIES['primary']() as sess:
        check('旧库经复制拿到新写入', sess.query(Batch).filter_by(name='新主库批次').count() == 1)
    check('读路由仍只选单一健康库', c.get('/switch/status', headers=h).json()['reader_node']
          in ('primary', 'standby'))

    print('7) 切换中断可续跑、重试同一单不重复换主')
    # 副库（下次切换目标=primary）中途断线
    o2 = switchover.initiate('admin@migration.local')
    set_node_outage('primary', True)
    interrupted = False
    try:
        switchover.run_switch(o2['id'])
    except Exception:
        interrupted = True
    set_node_outage('primary', False)
    check('中断时未翻转、写库仍为 standby', interrupted and ROUTER.writer_node() == 'standby')
    switchover.run_switch(o2['id'])  # 续跑同一单
    check('续跑后写库切回 primary', ROUTER.writer_node() == 'primary')
    check('任期仅再升一次到 3', ROUTER.term() == 3)
    with FACTORIES['primary']() as sess:
        from src.config.database import SwitchOrder
        check('切换单无重复', sess.query(SwitchOrder).count() == 2)

    print('8) 页面状态含当前写入位置与卡住原因')
    # 副库健康时先发起，再在追平中断线，验证中断态可观测、可续跑
    o3 = switchover.initiate('admin@migration.local')
    set_node_outage('standby', True)
    interrupted3 = False
    try:
        switchover.run_switch(o3['id'])
    except Exception:
        interrupted3 = True
    snap = switchover.status_snapshot()
    check('中断被检测并保留活动单', interrupted3 and snap['active_order_id'] == o3['id'])
    check('给出卡住原因', '连接中断' in snap['stuck_reason'])
    check('标明当前写入位置', snap['writer_node'] == 'primary')
    set_node_outage('standby', False)
    switchover.run_switch(o3['id'])
    check('恢复后续跑完成', switchover.get_order(o3['id']).status == 'completed')

    print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
    sys.exit(1 if FAIL else 0)


if __name__ == '__main__':
    main()
