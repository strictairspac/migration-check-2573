"""主副库切换状态机：页面发起、可中断续跑、幂等重试、旧主库栅栏。

阶段流转（每个阶段都先落库再改 Redis，进程崩溃后由 Router._bootstrap 重新入队续跑）：
  queued -> syncing（副库追平期间照常接单写主库）
        -> draining（短暂停写；最后一轮追平到零延迟；查询照常）
        -> flipping（翻转 writer 指针与任期号；旧主库之后写入被栅栏拒绝）
        -> completed（副库成为新主库，恢复接单）
"""
from datetime import datetime, timezone

from fastapi import HTTPException

from src.config.database import (
    ROUTER, SwitchOrder, FACTORIES, NodeMeta,
    NodeUnavailable, WritePaused, FencingError,
)
from src.config.queue_client import queue
from src.services.audit import log as audit_log

ACTIVE_STATUSES = ('queued', 'syncing', 'draining', 'flipping')

# 测试/验收钩子：进入某阶段时回调（可用于在 drain 窗口注入写入或中断）。
# 生产环境始终为 None。
phase_hook = None


def _hook(phase, order_id):
    if phase_hook is not None:
        phase_hook(phase, order_id)


def _order_in(node, order_id):
    with FACTORIES[node]() as s:
        return s.get(SwitchOrder, order_id)


def _find_unfinished_order():
    for node in ('primary', 'standby'):
        try:
            with FACTORIES[node]() as s:
                row = (s.query(SwitchOrder)
                       .filter(SwitchOrder.status.in_(ACTIVE_STATUSES))
                       .order_by(SwitchOrder.id.desc()).first())
                if row:
                    return row
        except Exception:
            continue
    return None


def _already_queued(order_id):
    return str(order_id) in queue.lrange(ROUTER.JOB_QUEUE, 0, -1)


def get_order(order_id):
    for node in ('primary', 'standby'):
        try:
            order = _order_in(node, order_id)
            if order:
                return order
        except Exception:
            continue
    raise HTTPException(404, '未找到切换单')


def initiate(requester):
    """页面发起切换。同一时刻只允许一个活动切换单；重试同一单返回原单，不重复换主。"""
    active_id = ROUTER.active_order_id()
    if active_id:
        existing = get_order(active_id)
        if existing.status in ACTIVE_STATUSES:
            return _order_record(existing, reused=True)
        # 残留活动指针但单子已终态：清理后允许新建
        queue.delete(ROUTER.KEY_ORDER)
        ROUTER.set_phase('idle')

    # 崩溃恢复：Redis 指针可能已翻转但回执丢失，库内仍有未完成单。
    # 找到它并重新入队续跑，而不是新建第二张单（避免重复换主）。
    unfinished = _find_unfinished_order()
    if unfinished is not None:
        queue.set(ROUTER.KEY_ORDER, str(unfinished.id))
        ROUTER.set_phase(unfinished.status, '检测到中断的切换单，自动续跑')
        if not _already_queued(unfinished.id):
            queue.rpush(ROUTER.JOB_QUEUE, unfinished.id)
        return _order_record(unfinished, reused=True)

    writer = ROUTER.writer_node()
    target = 'standby' if writer == 'primary' else 'primary'
    if not ROUTER.node_healthy(target):
        raise HTTPException(503, f'目标副库 {target} 不可用，无法发起切换；请先恢复节点')
    term = ROUTER.term()
    # 切换单写入当前主库（经复制日志同步到副库，切换后两边都查得到）
    with ROUTER.writer_session() as s:
        order = SwitchOrder(requester=requester, from_node=writer, target_node=target,
                            term=term, status='queued', reason='页面发起主副库交接')
        s.add(order)
        s.commit()
        order_id = order.id
    queue.set(ROUTER.KEY_ORDER, str(order_id))
    ROUTER.set_phase('queued', '切换单已排队，等待后台交接')
    queue.rpush(ROUTER.JOB_QUEUE, order_id)
    audit_log(requester, 'switch.initiate', f'#{order_id} {writer} -> {target}')
    return _order_record(get_order(order_id), reused=False)


def run_switch(order_id):
    """后台 worker 执行；任意阶段中断后重跑都从落库状态继续，幂等。"""
    order = get_order(order_id)
    if order.status == 'completed':
        return _order_record(order)
    if order.status == 'failed':
        raise RuntimeError('切换单已失败，请排查后在页面重新发起')

    target = order.target_node
    writer_at_start = order.from_node
    status = order.status

    # ---- syncing：副库追平，主库照常接单 -----------------------------------
    if status in ('queued', 'syncing'):
        _persist(order_id, status='syncing')
        ROUTER.set_phase('syncing', f'副库 {target} 正在追平复制日志，主库照常接单')
        _hook('syncing', order_id)
        _ensure_node(order_id, target)
        _sync_until_caught(order_id, target)
        status = 'syncing'  # 本阶段已落库完成

    # ---- draining：短暂停写，最后一轮追平到零延迟 ---------------------------
    if status in ('queued', 'syncing'):
        _persist(order_id, status='draining')
        ROUTER.set_phase('draining', f'交接 drain：写入短暂暂停，等待副库 {target} 零延迟')
        _hook('draining', order_id)
        _drain_to_zero(order_id, target)
        status = 'draining'

    # ---- flipping：翻转写指针与任期，旧主库永久栅栏 -------------------------
    if status in ('queued', 'syncing', 'draining', 'flipping'):
        # 先把 flipping 阶段状态复制到副库（此时仍零延迟），再翻转指针，
        # 保证进程在翻转前后任意时刻崩溃，都能从库内状态恢复且不重复换主。
        _persist(order_id, status='flipping')
        # 显式追平，确保 flipping 状态本身已复制到目标库后再翻转指针
        ROUTER.catch_up(target)
        assert ROUTER.replication_lag(target) == 0
        ROUTER.set_phase('flipping', f'翻转写库指针到 {target}，并提升任期号')
        _hook('flipping', order_id)
        new_term = order.term + 1
        max_lsn = ROUTER.writer_max_lsn()
        # 旧主库下线前记录已同步水位（其数据此刻与副库一致，之后不再接写）。
        # 用复制豁免会话，避免引擎写闸门拦截；这是翻转前对旧主库的最后一次内部写。
        if ROUTER.node_healthy(writer_at_start):
            with ROUTER.replay_session(writer_at_start) as s:
                meta = s.get(NodeMeta, 'applied_lsn')
                if meta:
                    meta.value = str(max_lsn)
                else:
                    s.add(NodeMeta(key='applied_lsn', value=str(max_lsn)))
                s.commit()
        # 指针翻转是单条 Redis 原子操作；崩溃恢复时以已完成订单的 target/term 重建
        queue.set(ROUTER.KEY_WRITER, target)
        queue.set(ROUTER.KEY_TERM, str(new_term))
        queue.delete(ROUTER.KEY_ORDER)
        # 翻转完成立即解除停写，后续回执/审计写入走新主库
        ROUTER.set_phase('idle')
        # 翻转后、回执落库前的崩溃注入点（模拟“切换回执丢失”）
        _hook('flipped', order_id)
        completed_at = datetime.now(timezone.utc)
        # 完成回执写到新主库（旧主库此时写入会被栅栏拒绝）
        with ROUTER.writer_session() as s:
            row = s.get(SwitchOrder, order_id)
            if row is None:
                # 新主库已通过 journal 重放该单；极端缺失时补建同号回执
                row = SwitchOrder(id=order_id, requester=order.requester,
                                  from_node=order.from_node, target_node=target,
                                  term=new_term)
                s.add(row)
            row.status = 'completed'
            row.detail = {**(row.detail or {}), 'completed_writer': target,
                          'new_term': new_term, 'final_lag': 0}
            row.term = new_term
            row.completed_at = completed_at
            s.commit()
            completed_record = _order_record(row)
        audit_log(order.requester, 'switch.completed',
                  f'#{order_id} {order.from_node} -> {target} term={new_term}')
        # 把新主库的完成回执/审计同步回已降级为副库的旧主库，保持双库零延迟
        if ROUTER.node_healthy(writer_at_start):
            ROUTER.catch_up(writer_at_start)
        return completed_record

    # 未进入翻转（中断恢复等场景）：返回当前库内最新状态
    return _order_record(get_order(order_id))


def _ensure_node(order_id, target):
    """节点中断不判失败：保留阶段状态、重新入队，节点恢复后从断点续跑。"""
    if ROUTER.node_healthy(target):
        return
    reason = f'目标副库 {target} 连接中断，交接已暂停；节点恢复后自动续跑，不重复换主'
    ROUTER.set_phase(ROUTER.phase(), reason)
    _persist(order_id, detail={'paused_reason': reason})
    queue.rpush(ROUTER.JOB_QUEUE, order_id)
    raise NodeUnavailable(target)


def _sync_until_caught(order_id, target):
    """syncing 阶段追平到零延迟。追平本身不产生业务日志，一轮即可收敛；
    仅在收敛后写一次阶段明细，避免回写日志造成延迟永不归零。"""
    _ensure_node(order_id, target)
    ROUTER.catch_up(target)
    lag = ROUTER.replication_lag(target)
    if lag != 0:  # 追平期间又有新写入：再来一轮（无回写日志，必然收敛）
        ROUTER.catch_up(target)
        lag = ROUTER.replication_lag(target)
    _persist(order_id, detail={
        'stage': 'sync', 'applied_lsn': ROUTER.applied_lsn(target),
        'writer_max_lsn': ROUTER.writer_max_lsn(), 'lag': lag})


def _drain_to_zero(order_id, target):
    """drain：写入已暂停，不会再有新业务日志，追平到零延迟（带兜底迭代上限）。"""
    for _ in range(50):
        _ensure_node(order_id, target)
        ROUTER.catch_up(target)
        lag = ROUTER.replication_lag(target)
        if lag == 0:
            _persist(order_id, detail={'stage': 'drain', 'lag': 0})
            return
    _fail(order_id, f'副库 {target} drain 阶段未能追平到零延迟')
    raise RuntimeError('drain 超时')


def _persist(order_id, status=None, detail=None):
    """把阶段进度写回当前写库的切换单（中断续跑的依据）。"""
    writer = ROUTER.writer_node()
    with FACTORIES[writer]() as s:
        row = s.get(SwitchOrder, order_id)
        if row is None:
            return
        if status:
            row.status = status
        if detail is not None:
            merged = dict(row.detail or {})
            merged.update(detail)
            row.detail = merged
        s.commit()


def _fail(order_id, reason):
    try:
        writer = ROUTER.writer_node()
        with FACTORIES[writer]() as s:
            row = s.get(SwitchOrder, order_id)
            if row:
                row.status = 'failed'
                row.reason = reason
                s.commit()
    except Exception:
        pass
    ROUTER.set_phase('idle', reason)
    queue.delete(ROUTER.KEY_ORDER)
    audit_log('system', 'switch.failed', f'#{order_id} {reason}')


def status_snapshot():
    snap = ROUTER.snapshot()
    active_id = snap['active_order_id']
    stuck = diagnose(snap)
    if active_id:
        try:
            snap['active_order'] = _order_record(get_order(active_id))
        except HTTPException:
            snap['active_order'] = None
    snap['stuck_reason'] = stuck
    # 最近切换单（两个库都能查到已完成单，这里读当前读路由）
    return snap


def diagnose(snap):
    """页面展示：当前写入位置与卡住原因。"""
    if snap['phase'] == 'idle':
        return ''
    target = snap['target_node']
    if not snap['nodes'][target]['healthy']:
        return f'目标副库 {target} 连接中断，交接暂停；节点恢复后将自动续跑，不会重复换主'
    if snap['phase'] == 'queued':
        return '切换单排队中，等待后台 worker 领取'
    if snap['phase'] == 'syncing':
        return f'副库追平中，还差 {snap["replication_lag"]} 条复制日志；主库仍可正常写入'
    if snap['phase'] == 'draining':
        return f'交接 drain 阶段：写入短暂暂停，副库还差 {snap["replication_lag"]} 条日志追平，查询不受影响'
    if snap['phase'] == 'flipping':
        return '正在翻转写库指针与任期号，毫秒级窗口'
    return snap.get('block_reason') or ''


def list_orders(limit=20):
    seen = {}
    for node in ('primary', 'standby'):
        try:
            with FACTORIES[node]() as s:
                for o in s.query(SwitchOrder).order_by(SwitchOrder.id.desc()).limit(limit).all():
                    seen[o.id] = _order_record(o)
        except Exception:
            continue
    return [seen[k] for k in sorted(seen, reverse=True)[:limit]]


def _order_record(order, reused=None):
    from src.utils.serialization import record
    data = record(order)
    if reused is not None:
        data['idempotent_reuse'] = reused
    return data
