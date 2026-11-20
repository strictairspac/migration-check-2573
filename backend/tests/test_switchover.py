"""切换状态机：发起、追平、停写、翻转、中断续跑、幂等与旧主库栅栏。"""
import pytest

from src.config.database import (
    FACTORIES, ROUTER, SwitchOrder, set_node_outage,
    WritePaused, FencingError, NodeUnavailable,
)
from src.models.batch import Batch
from src.services import switchover


def _new_batch(name):
    with ROUTER.writer_session() as s:
        s.add(Batch(name=name, rows=[], source_file=name + '.json', description=''))
        s.commit()


def test_full_switchover_through_all_phases(app_env):
    _new_batch('切换前写入-1')
    order = switchover.initiate('admin@migration.local')
    oid = order['id']
    assert order['from_node'] == 'primary'
    assert order['target_node'] == 'standby'
    assert order['status'] == 'queued'
    assert ROUTER.phase() == 'queued'

    seen = []

    def hook(phase, order_id):
        assert order_id == oid
        seen.append(phase)
        if phase == 'syncing':
            # 副库追平期间主库照常接单
            _new_batch('syncing期接单')
            assert ROUTER.writer_node() == 'primary'
        elif phase == 'draining':
            # drain 阶段写入闸门已生效（在钩子内直接验证，不向外抛异常）
            try:
                with ROUTER.writer_session() as s:
                    s.add(Batch(name='drain期写入', rows=[], source_file='d.json', description=''))
                    s.commit()
                raise AssertionError('drain 阶段写入应当被暂停')
            except WritePaused:
                pass
            # 查询照常
            node, rs = ROUTER.reader_session()
            with rs:
                assert rs.query(Batch).filter_by(name='切换前写入-1').count() == 1

    switchover.phase_hook = hook
    try:
        result = switchover.run_switch(oid)
    finally:
        switchover.phase_hook = None

    assert seen == ['syncing', 'draining', 'flipping', 'flipped']
    assert result['status'] == 'completed'
    assert ROUTER.writer_node() == 'standby'
    assert ROUTER.term() == 2
    assert ROUTER.phase() == 'idle'

    # 副库（新主库）数据齐全：追平前、syncing 期写入都在；drain 期写入被拒
    assert ROUTER.replication_lag('primary') == 0
    with FACTORIES['standby']() as s:
        assert s.query(Batch).filter_by(name='切换前写入-1').count() == 1
        assert s.query(Batch).filter_by(name='syncing期接单').count() == 1
        assert s.query(Batch).filter_by(name='drain期写入').count() == 0
        assert s.query(SwitchOrder).filter_by(id=oid).one().status == 'completed'


def test_idempotent_retry_does_not_double_switch(app_env):
    first = switchover.initiate('admin@migration.local')
    # 活动期重复发起：返回同一单，不产生第二张单、不重复换主
    again = switchover.initiate('admin@migration.local')
    assert again['id'] == first['id']
    assert again['idempotent_reuse'] is True
    with FACTORIES[ROUTER.writer_node()]() as s:
        assert s.query(SwitchOrder).count() == 1

    switchover.run_switch(first['id'])
    assert ROUTER.writer_node() == 'standby'
    # 完成后再“重试同一单”：直接返回已完成单，不再翻转
    done = switchover.run_switch(first['id'])
    assert done['status'] == 'completed'
    assert ROUTER.writer_node() == 'standby'
    assert ROUTER.term() == 2
    with FACTORIES['standby']() as s:
        assert s.query(SwitchOrder).count() == 1


def test_old_primary_fenced_after_reconnect(app_env):
    _new_batch('栅栏前数据')
    order = switchover.initiate('admin@migration.local')
    switchover.run_switch(order['id'])
    assert ROUTER.writer_node() == 'standby'

    # 旧主库重新上线（健康），但绝不能再接写
    set_node_outage('primary', False)
    assert ROUTER.node_healthy('primary') is True
    from src.config.database import PrimarySession
    # 直连旧主库构造写会话并提交：任期栅栏必须回滚
    sess = PrimarySession()
    sess.add(Batch(name='旧主库越权写入', rows=[], source_file='old.json', description=''))
    with pytest.raises(FencingError):
        sess.commit()
    sess.rollback()
    with FACTORIES['primary']() as s:
        assert s.query(Batch).filter_by(name='旧主库越权写入').count() == 0

    # 新主库写入正常，并通过复制回到已降级为副库的旧主库
    _new_batch('新主库写入')
    ROUTER.catch_up('primary')
    with FACTORIES['primary']() as s:
        assert s.query(Batch).filter_by(name='新主库写入').count() == 1


def test_switch_resumes_after_interruption(app_env):
    """副库在 syncing 中断：单子暂停并重新入队，恢复后从断点续跑到完成。"""
    _new_batch('中断前写入')
    order = switchover.initiate('admin@migration.local')
    oid = order['id']

    # 让副库在追平前断线，驱动一次：应抛 NodeUnavailable 且订单保留在 syncing
    set_node_outage('standby', True)
    with pytest.raises(NodeUnavailable):
        switchover.run_switch(oid)
    assert switchover.get_order(oid).status == 'syncing'
    # 没有发生翻转，写指针仍是旧主库，写入继续可用
    assert ROUTER.writer_node() == 'primary'
    _new_batch('中断期间主库接单')

    # 队列里应保留/重新入队同一单（续跑而非新单）
    queued = app_env['queue'].lrange(ROUTER.JOB_QUEUE, 0, -1)
    assert str(oid) in queued

    # 副库恢复，续跑到完成；全程只有一张切换单
    set_node_outage('standby', False)
    switchover.run_switch(oid)
    assert switchover.get_order(oid).status == 'completed'
    assert ROUTER.writer_node() == 'standby'
    with FACTORIES['standby']() as s:
        assert s.query(SwitchOrder).count() == 1
        assert s.query(Batch).filter_by(name='中断期间主库接单').count() == 1


def test_switch_blocked_when_target_down_at_initiate(app_env):
    from fastapi import HTTPException
    set_node_outage('standby', True)
    try:
        with pytest.raises(HTTPException) as ei:
            switchover.initiate('admin@migration.local')
        assert ei.value.status_code == 503
    finally:
        set_node_outage('standby', False)
    assert ROUTER.phase() == 'idle'


def test_status_reports_writer_and_stuck_reason(app_env):
    order = switchover.initiate('admin@migration.local')
    set_node_outage('standby', True)
    with pytest.raises(NodeUnavailable):
        switchover.run_switch(order['id'])
    snap = switchover.status_snapshot()
    assert snap['writer_node'] == 'primary'
    assert snap['active_order_id'] == order['id']
    assert '连接中断' in snap['stuck_reason']
    set_node_outage('standby', False)
