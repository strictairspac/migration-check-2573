"""双库路由：主库/副库引擎、读写路由、写入闸门、任期栅栏与应用级逻辑复制。

设计要点
- 一次会话只绑定一个引擎：写走当前主库；读路由只选一个库，绝不扇出两个库。
- 写库会话提交前检查协调状态：drain/flipping 阶段短暂停写（WritePaused，可重试）；
  任期号或写节点变化触发 FencingError——旧主库重新上线也永久拒写。
- 业务表行变更经 after_flush 写入 replication_journal（同事务），LSN 全局单调，
  由应用在节点间同步序列；副库按 LSN 顺序重放幂等追平。
- 协调状态以 Redis 为权威，进程重启/中断时从库内 switch_orders 引导并续跑。
"""
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, JSON, DateTime, Text, func, create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

from src.config.settings import PRIMARY_DATABASE_URL, STANDBY_DATABASE_URL
from src.config.queue_client import queue

Base = declarative_base()


class ReplicationJournal(Base):
    __tablename__ = 'replication_journal'
    lsn = Column(Integer, primary_key=True)          # 全局单调日志序号
    table_name = Column(String(60), nullable=False)
    op = Column(String(10), nullable=False)          # insert/update/delete
    row_pk = Column(String(60), nullable=False)
    payload = Column(JSON, nullable=False)
    term = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class NodeMeta(Base):
    __tablename__ = 'node_meta'
    key = Column(String(60), primary_key=True)
    value = Column(Text, nullable=False)


class SwitchOrder(Base):
    __tablename__ = 'switch_orders'
    id = Column(Integer, primary_key=True, autoincrement=True)
    requester = Column(String(120), nullable=False)
    from_node = Column(String(20), nullable=False)
    target_node = Column(String(20), nullable=False)
    term = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default='queued')  # queued/syncing/draining/flipping/completed/failed
    reason = Column(String(300), nullable=True)
    detail = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime, nullable=True)


def _engine(url):
    if url.startswith('sqlite'):
        from sqlalchemy import event as _event
        eng = create_engine(
            url,
            connect_args={'check_same_thread': False, 'timeout': 30},
            pool_pre_ping=True,
        )

        @_event.listens_for(eng, 'connect')
        def _sqlite_pragmas(dbapi_conn, _rec):
            cur = dbapi_conn.cursor()
            cur.execute('PRAGMA journal_mode=WAL')
            cur.execute('PRAGMA busy_timeout=30000')
            cur.execute('PRAGMA synchronous=NORMAL')
            cur.close()

        return eng
    return create_engine(url, pool_pre_ping=True)


primary_engine = _engine(PRIMARY_DATABASE_URL)
standby_engine = _engine(STANDBY_DATABASE_URL)
ENGINES = {'primary': primary_engine, 'standby': standby_engine}
NODE_NAMES = ('primary', 'standby')


class NodeUnavailable(RuntimeError):
    def __init__(self, node):
        super().__init__(f'数据库节点 {node} 当前不可用（网络中断或未上线）')
        self.node = node


class WritePaused(RuntimeError):
    """交接 drain 阶段短暂停写，调用方稍后重试即可。"""


class FencingError(RuntimeError):
    """任期栅栏：旧主库重新上线后也不允许接写。"""


# 连接级写闸门：非当前写库节点上的任何写语句都被拒绝（复制重放显式豁免）。
# 这样即使绕过 writer_session 直连旧主库引擎，旧库重新上线后也无法接写。
_WRITE_PREFIXES = ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'MERGE')


# 复制重放豁免：线程局部上下文。重放同步执行，仅在 with 块内放行当前线程的写闸门，
# 退出即恢复，不污染连接池里的连接（连接 info 会随连接复用而泄漏）。
_replay_local = threading.local()


class replay_context:
    def __enter__(self):
        _replay_local.active = True
        return self

    def __exit__(self, *exc):
        _replay_local.active = False
        return False


def install_write_gate(node, eng):
    @event.listens_for(eng, 'before_cursor_execute')
    def _gate(conn, _cursor, statement, _parameters, _context, _many):
        if ROUTER is None:
            return
        if getattr(_replay_local, 'active', False) or conn.info.get('replay_allowed'):
            return
        if not statement.lstrip().upper().startswith(_WRITE_PREFIXES):
            return
        if ROUTER.writer_node() != node:
            raise FencingError(
                f'节点 {node} 不是当前主库（当前主库 {ROUTER.writer_node()}），'
                f'写语句已在连接层拒绝')
PrimarySession = sessionmaker(bind=primary_engine)
StandbySession = sessionmaker(bind=standby_engine)
FACTORIES = {'primary': PrimarySession, 'standby': StandbySession}

# 向后兼容旧代码
engine = primary_engine
Session = PrimarySession

# ---- 故障注入（仅本地测试；生产环境没有该开关） ------------------------------
_injected_outage = set()
_outage_lock = threading.RLock()


def set_node_outage(node, down):
    with _outage_lock:
        (_injected_outage.add if down else _injected_outage.discard)(node)


def node_in_outage(node):
    with _outage_lock:
        return node in _injected_outage


_JOURNAL_SKIP = {'replication_journal', 'node_meta'}


def _jsonable(v):
    return v.isoformat() if isinstance(v, datetime) else v


def _serialize(obj):
    return {c.name: _jsonable(getattr(obj, c.name)) for c in obj.__table__.columns}


def install_journal_listener(factory, node):

    @event.listens_for(factory, 'after_flush')
    def _after_flush(session, _ctx):
        if session.info.get('_replay'):
            return  # 复制重放不再产生新 journal，避免自引用
        if ROUTER is None or ROUTER.writer_node() != node:
            return
        if session.info.get('_journaling'):
            return
        term = ROUTER.term()
        entries = []
        for obj in list(session.new):
            if obj.__tablename__ in _JOURNAL_SKIP:
                continue
            pk = obj.__mapper__.primary_key_from_instance(obj)[0]
            entries.append(('insert', obj.__tablename__, str(pk), _serialize(obj)))
        for obj in list(session.dirty):
            if obj.__tablename__ in _JOURNAL_SKIP or not session.is_modified(obj):
                continue
            pk = obj.__mapper__.primary_key_from_instance(obj)[0]
            entries.append(('update', obj.__tablename__, str(pk), _serialize(obj)))
        for obj in list(session.deleted):
            if obj.__tablename__ in _JOURNAL_SKIP:
                continue
            pk = obj.__mapper__.primary_key_from_instance(obj)[0]
            entries.append(('delete', obj.__tablename__, str(pk), _serialize(obj)))
        if not entries:
            return
        session.info['_journaling'] = True
        try:
            # after_flush 内禁止再调 flush；新增的日志行保持 pending，
            # 随本次事务提交时由 SQLAlchemy 自动刷出（同一事务、原子可见）。
            for op, table_name, pk, payload in entries:
                session.add(ReplicationJournal(
                    lsn=ROUTER.next_lsn(), table_name=table_name, op=op,
                    row_pk=pk, payload=payload, term=term))
        finally:
            session.info['_journaling'] = False


class Router:
    KEY_WRITER = 'db:writer_node'
    KEY_TERM = 'db:term'
    KEY_PHASE = 'db:switch_phase'
    KEY_ORDER = 'db:active_order'
    KEY_REASON = 'db:block_reason'
    KEY_LSN = 'db:next_lsn'
    JOB_QUEUE = 'switchover-jobs'

    def __init__(self, q):
        self.q = q
        self._lsn_lock = threading.Lock()
        self._bootstrapped = False

    # ---- 节点健康 -----------------------------------------------------------
    def check_node(self, node):
        if node_in_outage(node):
            raise NodeUnavailable(node)
        with ENGINES[node].connect() as conn:
            conn.exec_driver_sql('SELECT 1')

    def node_healthy(self, node):
        try:
            self.check_node(node)
            return True
        except Exception:
            return False

    # ---- 协调状态引导 -------------------------------------------------------
    def _bootstrap(self):
        if self._bootstrapped:
            return
        if not self.q.exists(self.KEY_WRITER):
            order = self._latest_order()
            if order and order.status == 'completed':
                self.q.set(self.KEY_WRITER, order.target_node)
                self.q.set(self.KEY_TERM, str(order.term))
                self.q.set(self.KEY_PHASE, 'idle')
            else:
                self.q.set(self.KEY_WRITER, 'primary')
                self.q.set(self.KEY_TERM, '1')
                if order and order.status in ('queued', 'syncing', 'draining', 'flipping'):
                    # 中断的切换：恢复阶段并重新入队续跑（同一单不重复换主）
                    self.q.set(self.KEY_PHASE, order.status)
                    self.q.set(self.KEY_ORDER, str(order.id))
                    self.q.rpush(self.JOB_QUEUE, order.id)
                else:
                    self.q.set(self.KEY_PHASE, 'idle')
            self.q.set(self.KEY_LSN, str(self._max_journal_lsn_anywhere()))
        self._bootstrapped = True

    def _latest_order(self):
        for node in NODE_NAMES:
            try:
                with FACTORIES[node]() as s:
                    return s.query(SwitchOrder).order_by(SwitchOrder.id.desc()).first()
            except Exception:
                continue
        return None

    def _max_journal_lsn_anywhere(self):
        m = 0
        for node in NODE_NAMES:
            try:
                with FACTORIES[node]() as s:
                    row = s.query(ReplicationJournal).order_by(ReplicationJournal.lsn.desc()).first()
                    m = max(m, row.lsn if row else 0)
            except Exception:
                continue
        return m

    # ---- 协调状态读取 -------------------------------------------------------
    def writer_node(self):
        self._bootstrap()
        return self.q.get(self.KEY_WRITER) or 'primary'

    def term(self):
        self._bootstrap()
        return int(self.q.get(self.KEY_TERM) or '1')

    def phase(self):
        self._bootstrap()
        return self.q.get(self.KEY_PHASE) or 'idle'

    def active_order_id(self):
        self._bootstrap()
        v = self.q.get(self.KEY_ORDER)
        return int(v) if v else None

    def block_reason(self):
        return self.q.get(self.KEY_REASON) or ''

    def set_phase(self, phase, reason=''):
        self.q.set(self.KEY_PHASE, phase)
        if reason:
            self.q.set(self.KEY_REASON, reason)
        elif phase == 'idle':
            self.q.delete(self.KEY_REASON)

    def next_lsn(self):
        with self._lsn_lock:
            v = self.q.incr(self.KEY_LSN)
            return int(v)

    # ---- 会话工厂 -----------------------------------------------------------
    def writer_session(self):
        """写会话：绑定当前主库。

        draining 阶段在入口即拒绝（明确的停写窗口，调用方退避重试）；
        flipping 是毫秒级翻转窗口，入口放行、由提交前栅栏裁决，避免线程
        在翻转瞬间建会话被误伤后无法自愈。
        """
        node = self.writer_node()
        self.check_node(node)
        if self.phase() == 'draining':
            raise WritePaused(self.block_reason() or '主副库交接中，写入短暂暂停，请稍后重试')
        bound_node, bound_term = node, self.term()
        session = FACTORIES[node]()

        @event.listens_for(session, 'before_commit')
        def _guard(_s):
            if self.writer_node() != bound_node or self.term() != bound_term:
                raise FencingError('写入落在旧主库：主库任期已变化，本次写入已被栅栏拒绝并回滚')
            if self.phase() in ('draining', 'flipping'):
                raise WritePaused('主副库交接 drain 阶段，写入已暂停，可稍后重试')

        return session

    def reader_route(self):
        """选读库：一次查询只绑定一个库。副库健康且零延迟读副库，否则读主库。"""
        writer = self.writer_node()
        candidate = 'standby' if writer == 'primary' else 'primary'
        if self.node_healthy(candidate) and self.replication_lag(candidate) == 0:
            return candidate
        return writer

    def reader_session(self):
        node = self.reader_route()
        self.check_node(node)
        return node, FACTORIES[node]()

    # ---- 复制与追平 ---------------------------------------------------------
    def applied_lsn(self, node):
        with FACTORIES[node]() as s:
            meta = s.get(NodeMeta, 'applied_lsn')
            return int(meta.value) if meta else 0

    def writer_max_lsn(self):
        node = self.writer_node()
        with FACTORIES[node]() as s:
            row = s.query(ReplicationJournal).order_by(ReplicationJournal.lsn.desc()).first()
            return row.lsn if row else 0

    def replication_lag(self, node):
        try:
            return max(0, self.writer_max_lsn() - self.applied_lsn(node))
        except Exception:
            return None

    def catch_up(self, standby='standby', batch=200, progress=None):
        """把写库 journal 重放到目标库直到追平；按 applied_lsn 续跑，天然幂等。

        重放按主键 upsert、水位用 merge 更新，重复执行收敛到同一结果。
        """
        self.check_node(standby)
        while True:
            writer = self.writer_node()
            applied = self.applied_lsn(standby)
            with FACTORIES[writer]() as rs:
                entries = (rs.query(ReplicationJournal)
                           .filter(ReplicationJournal.lsn > applied)
                           .order_by(ReplicationJournal.lsn).limit(batch).all())
                if not entries:
                    return applied
                packed = [(e.lsn, e.table_name, e.op, e.row_pk, e.payload) for e in entries]
                last_lsn = entries[-1].lsn
            self._replay(standby, packed)
            with self.replay_session(standby) as ws:
                ws.merge(NodeMeta(key='applied_lsn', value=str(last_lsn)))
                ws.commit()
            if progress:
                progress(last_lsn)

    @contextmanager
    def replay_session(self, node):
        """复制重放专用会话：上下文内当前线程的写闸门放行，退出即恢复。

        journal 监听器对带 _replay 标记的会话短路，重放只应用既有日志、
        不再产生新的复制日志，避免自引用。
        """
        session = FACTORIES[node]()
        session.info['_replay'] = True
        with replay_context():
            try:
                yield session
            finally:
                session.close()

    def _replay(self, node, entries):
        """逐条独立事务重放，避免同批次 insert/delete 在会话身份映射中相互抵消；
        每条按主键 upsert/delete 幂等，崩溃后从 applied_lsn 续跑不会重复生效。"""
        from src.models.registry import REGISTRY
        for lsn, table_name, op, row_pk, payload in entries:
            model = REGISTRY.get(table_name)
            if model is None:
                continue
            pk_name = model.__table__.primary_key.columns[0].name
            pk_value = _coerce_pk(model, payload.get(pk_name))
            with self.replay_session(node) as s:
                existing = s.get(model, pk_value)
                if op == 'insert':
                    if existing is None:
                        s.add(model(**_coerce_payload(model, payload)))
                    else:
                        for k, v in _coerce_payload(model, payload).items():
                            setattr(existing, k, v)
                elif op == 'update':
                    if existing is not None:
                        for k, v in _coerce_payload(model, payload).items():
                            setattr(existing, k, v)
                elif op == 'delete':
                    if existing is not None:
                        s.delete(existing)
                s.commit()

    # ---- 页面状态快照 -------------------------------------------------------
    def snapshot(self):
        writer = self.writer_node()
        target = 'standby' if writer == 'primary' else 'primary'
        reader = self.reader_route()
        nodes = {}
        for n in NODE_NAMES:
            healthy = self.node_healthy(n)
            nodes[n] = {
                'healthy': healthy,
                'role': 'writer' if n == writer else 'standby',
                'applied_lsn': self.applied_lsn(n) if healthy else None,
            }
        return {
            'writer_node': writer,
            'reader_node': reader,
            'target_node': target,
            'term': self.term(),
            'phase': self.phase(),
            'active_order_id': self.active_order_id(),
            'block_reason': self.block_reason(),
            'writer_max_lsn': self.writer_max_lsn() if self.node_healthy(writer) else None,
            'replication_lag': self.replication_lag(target),
            'nodes': nodes,
            'server_time': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        }


def _coerce_pk(model, value):
    col = model.__table__.primary_key.columns[0]
    if isinstance(col.type, Integer) and isinstance(value, str):
        return int(value)
    return value


def _coerce_payload(model, payload):
    from datetime import datetime
    out = {}
    for col in model.__table__.columns:
        if col.name not in payload:
            continue
        v = payload[col.name]
        if isinstance(col.type, Integer) and isinstance(v, str):
            v = int(v)
        elif isinstance(col.type, DateTime) and isinstance(v, str):
            v = datetime.fromisoformat(v)
        out[col.name] = v
    return out


ROUTER = Router(queue)

# ROUTER 就绪后安装连接级写闸门（闭包在此刻能解析到 ROUTER 与 FencingError）
for _node, _engine in ENGINES.items():
    install_write_gate(_node, _engine)


def init_db(_queue=None):
    """双库建表、引导协调状态并挂接复制日志监听（幂等）。"""
    for eng in ENGINES.values():
        Base.metadata.create_all(eng)
    ROUTER._bootstrap()
    if not getattr(init_db, '_listeners', False):
        install_journal_listener(PrimarySession, 'primary')
        install_journal_listener(StandbySession, 'standby')
        init_db._listeners = True
    return ROUTER
