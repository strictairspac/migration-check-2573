"""复制日志、追平与读写路由。"""
from src.config.database import (
    FACTORIES, ReplicationJournal, NodeMeta, set_node_outage,
)
from src.models.batch import Batch
from src.models.run import Run, TargetRow


def test_writes_go_to_writer_and_journaled(app_env):
    ROUTER = app_env['db'].ROUTER
    assert ROUTER.writer_node() == 'primary'
    with ROUTER.writer_session() as s:
        s.add(Batch(name='复制探针批次', rows=[], source_file='p.json', description='x'))
        s.commit()
    with FACTORIES['primary']() as s:
        assert s.query(Batch).filter_by(name='复制探针批次').count() == 1
        j = next(x for x in s.query(ReplicationJournal).filter_by(
            table_name='batches', op='insert').all()
            if x.payload.get('name') == '复制探针批次')
        assert j.term == 1 and j.lsn >= 1
    # 副库尚未追平：能查到延迟
    assert ROUTER.replication_lag('standby') >= 1


def test_catch_up_replays_and_is_idempotent(app_env):
    ROUTER = app_env['db'].ROUTER
    with ROUTER.writer_session() as s:
        s.add(Batch(name='追平批次A', rows=[], source_file='a.json', description=''))
        s.add(Batch(name='追平批次B', rows=[], source_file='b.json', description=''))
        s.commit()
    lag = ROUTER.replication_lag('standby')
    assert lag >= 2
    ROUTER.catch_up('standby')
    assert ROUTER.replication_lag('standby') == 0
    with FACTORIES['standby']() as s:
        assert s.query(Batch).filter(Batch.name.in_(['追平批次A', '追平批次B'])).count() == 2
    # 再次追平是幂等空操作，不产生重复行
    ROUTER.catch_up('standby')
    with FACTORIES['standby']() as s:
        assert s.query(Batch).filter(Batch.name.in_(['追平批次A', '追平批次B'])).count() == 2


def test_update_and_delete_replay(app_env):
    ROUTER = app_env['db'].ROUTER
    with ROUTER.writer_session() as s:
        b = Batch(name='变更批次', rows=[{'x': 1}], source_file='c.json', description='旧')
        s.add(b)
        s.commit()
    with ROUTER.writer_session() as s:
        b = s.query(Batch).filter_by(name='变更批次').one()
        b.description = '新'
        s.commit()
    with ROUTER.writer_session() as s:
        b = s.query(Batch).filter_by(name='变更批次').one()
        s.delete(b)
        s.commit()
    ROUTER.catch_up('standby')
    with FACTORIES['standby']() as s:
        gone = s.query(Batch).filter_by(name='变更批次').first()
        assert gone is None
        # 副库以 applied_lsn 记录重放水位（日志表本身不重放，避免自引用）
        assert int(s.get(NodeMeta, 'applied_lsn').value) == ROUTER.writer_max_lsn()
    # 通过 payload 名称精确定位本批次的 insert/update/delete 三条日志
    with FACTORIES['primary']() as s:
        ops = [j.op for j in s.query(ReplicationJournal).filter_by(table_name='batches')
               .order_by(ReplicationJournal.lsn).all()
               if j.payload.get('name') == '变更批次']
        assert ops == ['insert', 'update', 'delete']


def test_reader_uses_one_node_and_falls_back_when_lagging(app_env):
    ROUTER = app_env['db'].ROUTER
    # 有延迟时读主库
    with ROUTER.writer_session() as s:
        s.add(Batch(name='延迟读批次', rows=[], source_file='r.json', description=''))
        s.commit()
    assert ROUTER.reader_route() == 'primary'
    # 追平后零延迟：读副库（同一次查询只绑定一个库）
    ROUTER.catch_up('standby')
    assert ROUTER.reader_route() == 'standby'
    node, session = ROUTER.reader_session()
    with session:
        assert node == 'standby'
        assert session.query(Batch).filter_by(name='延迟读批次').count() == 1


def test_reader_falls_back_when_standby_down(app_env):
    ROUTER = app_env['db'].ROUTER
    ROUTER.catch_up('standby')
    assert ROUTER.reader_route() == 'standby'
    set_node_outage('standby', True)
    try:
        assert ROUTER.reader_route() == 'primary'  # 查询照常，落到健康主库
    finally:
        set_node_outage('standby', False)
