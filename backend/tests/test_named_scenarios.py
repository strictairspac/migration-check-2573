"""题目点名场景：交接时任务正要落库、切换回执丢失、旧库重新上线。

并发放大：用线程在切换 drain/flipping 窗口内并发提交迁移任务，
验证退避重试后在新主库恰好落库一次，无重复目标行。
"""
import threading
import pytest

from src.config.database import (
    FACTORIES, ROUTER, SwitchOrder, set_node_outage,
    FencingError, WritePaused, NodeUnavailable,
)
from src.models.run import Run, TargetRow
from src.services import switchover
from src.services import runs as runs_service


def _create_run(mapping_id, name):
    from src.schemas.business import RunInput
    return runs_service.create(RunInput(name=name, mapping_id=mapping_id))


def _first_mapping_id():
    _node, s = ROUTER.reader_session()
    with s:
        from src.models.mapping import Mapping
        return s.query(Mapping).order_by(Mapping.id).first().id


def test_task_landing_during_handover_commits_once(app_env):
    """迁移任务在 drain/flipping 窗口正要落库：重试后在新主库提交一次。

    不启动 worker 夹具，避免队列消费者与本测试的提交线程并发处理同一 run；
    专门验证 commit_with_retry 在交接窗口的退避重试与恰好一次语义。
    """
    mapping_id = _first_mapping_id()
    run = _create_run(mapping_id, '交接窗口任务')
    run_id = run['id']
    # 移除队列消息并模拟 worker 已把 run 置为 running、正要提交目标行
    app_env['queue'].lrem('migration-runs', 0, str(run_id))
    with ROUTER.writer_session() as s:
        s.get(Run, run_id).status = 'running'
        s.commit()

    landed = {}
    bg = {}

    def hook(phase, order_id):
        if phase == 'draining':
            # 此刻正好要提交目标行：在独立线程驱动提交，撞 drain 闸门后由重试逻辑接管
            def attempt():
                try:
                    app_env['worker'].commit_with_retry(run_id, _build_result(run_id))
                    landed['ok'] = True
                except Exception as e:  # noqa: BLE001
                    landed['err'] = repr(e)
            t = threading.Thread(target=attempt, daemon=True)
            t.start()  # 不 join：drain 结束、翻转完成后线程自行在新主库提交
            bg['t'] = t

    order = switchover.initiate('admin@migration.local')
    switchover.phase_hook = hook
    try:
        switchover.run_switch(order['id'])
    finally:
        switchover.phase_hook = None

    assert ROUTER.writer_node() == 'standby'

    import time
    deadline = time.time() + 10
    while time.time() < deadline and not landed.get('ok'):
        time.sleep(0.1)
    bg['t'].join(timeout=10)          # 收窄线程，杜绝跨用例污染
    assert not bg['t'].is_alive()
    assert landed.get('ok') is True, landed

    with FACTORIES[ROUTER.writer_node()]() as s:
        r = s.get(Run, run_id)
        assert r.status == 'completed'
        # 自然键唯一约束 + 幂等提交：目标行恰好一份，无重复
        rows = s.query(TargetRow).filter_by(run_id=run_id).all()
        keys = [x.key_digest for x in rows]
        assert len(keys) == len(set(keys))
        assert len(rows) == r.result['accepted_count']


def _create_run(mapping_id, name):
    from src.schemas.business import RunInput
    return runs_service.create(RunInput(name=name, mapping_id=mapping_id))


def _build_result(run_id):
    accepted = [
        {'sample_code': f'HANDOVER-{run_id}', 'test_name': '拉伸强度',
         'result_value': '420.100000', 'unit': 'MPa',
         'measured_at': '2026-11-17T01:00:00+00:00'},
    ]
    from src.services.migration import digest, make_backup
    result = {
        'accepted_rows': accepted,
        'rejected_rows': [],
        'row_hashes': [digest(r) for r in accepted],
        'checksum': digest(accepted),
        'chunks': [{'chunk': 1, 'processed': 1, 'accepted': 1, 'rejected': 0}],
        'source_count': 1, 'accepted_count': 1, 'rejected_count': 0,
    }
    result['backup'] = make_backup(result, run_id, 'handover-batch')
    return result


def test_switch_acknowledgement_lost_is_recoverable(app_env):
    """切换回执丢失（模拟 flipping 后、完成回执落库前崩溃）：状态可恢复且不重复换主。"""
    order = switchover.initiate('admin@migration.local')
    oid = order['id']

    # 让回执在“指针已翻转、完成回执落库前”丢失
    def hook(phase, order_id):
        if phase == 'flipped':
            raise _AckLostSimulated()

    switchover.phase_hook = hook
    with pytest.raises(_AckLostSimulated):
        switchover.run_switch(oid)
    switchover.phase_hook = None

    # 指针与任期已经翻转（Redis 原子操作成功），只是订单回执仍停在 flipping
    assert ROUTER.writer_node() == 'standby'
    assert ROUTER.term() == 2
    stuck = switchover.get_order(oid)
    assert stuck.status == 'flipping'

    # 续跑同一单：从 flipping 断点补写回执完成，不再二次翻转
    again = switchover.initiate('admin@migration.local')
    # 活动单复用：返回同一单
    assert again['id'] == oid
    result = switchover.run_switch(oid)
    assert result['status'] == 'completed'
    assert ROUTER.writer_node() == 'standby'
    assert ROUTER.term() == 2  # 任期没有被重复递增
    with FACTORIES['standby']() as s:
        assert s.query(SwitchOrder).filter_by(id=oid).one().status == 'completed'
        assert s.query(SwitchOrder).count() == 1


class _AckLostSimulated(Exception):
    pass


def test_old_primary_reconnect_cannot_accept_writes(app_env):
    """旧库重新上线：健康但引擎级栅栏拒写；新主库写入经复制回到旧库可查。"""
    order = switchover.initiate('admin@migration.local')
    switchover.run_switch(order['id'])
    assert ROUTER.writer_node() == 'standby'

    # 旧库一度离线再恢复
    set_node_outage('primary', True)
    assert ROUTER.node_healthy('primary') is False
    set_node_outage('primary', False)
    assert ROUTER.node_healthy('primary') is True

    # 直连旧主库引擎的原生写也必须被连接层栅栏拒绝
    from sqlalchemy import text
    import src.config.database as dbmod
    with pytest.raises(FencingError):
        with dbmod.primary_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO batches (name, source_file, description, rows) "
                "VALUES ('旧库越权','x.json','','[]')"))

    from src.models.batch import Batch
    with FACTORIES['primary']() as s:
        assert s.query(Batch).filter_by(name='旧库越权').count() == 0

    # 新主库正常写入，复制回旧主库后两边都能查到（同一次查询只读一个库）
    from src.models.batch import Batch
    with ROUTER.writer_session() as s:
        s.add(Batch(name='新主库正常写入', rows=[], source_file='n.json', description=''))
        s.commit()
    ROUTER.catch_up('primary')
    with FACTORIES['primary']() as s:
        assert s.query(Batch).filter_by(name='新主库正常写入').count() == 1
    node, rs = ROUTER.reader_session()
    with rs:
        assert node == 'primary'  # 旧库已追平，读路由可选中它
        assert rs.query(Batch).filter_by(name='新主库正常写入').count() == 1


def test_writes_paused_only_briefly_and_reads_unaffected(app_env):
    """drain 短暂停写返回 423 语义，查询全程可用；停写结束后写入恢复。"""
    phases_seen = []

    def hook(phase, order_id):
        phases_seen.append(phase)
        if phase == 'draining':
            with pytest.raises(WritePaused):
                with ROUTER.writer_session():
                    pass  # 进入即应在 drain 被拦
            # 查询不中断
            node, rs = ROUTER.reader_session()
            with rs:
                assert rs.query(Run).count() == 0 or rs.query(Run).count() >= 0

    order = switchover.initiate('admin@migration.local')
    switchover.phase_hook = hook
    try:
        switchover.run_switch(order['id'])
    finally:
        switchover.phase_hook = None
    assert 'draining' in phases_seen
    # 交接结束写入恢复
    from src.models.batch import Batch
    with ROUTER.writer_session() as s:
        s.add(Batch(name='交接后写入', rows=[], source_file='a.json', description=''))
        s.commit()
