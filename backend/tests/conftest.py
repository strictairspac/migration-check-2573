"""主副库切换验收测试的共享夹具。

进程内加载一次应用；每个测试清空 fakeredis 与双 sqlite 库后重建，
worker 以短轮询后台线程处理 migration-runs / switchover-jobs 两个队列。
"""
import os
import sys
import time
import threading
import pathlib

import pytest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

TEST_DATA_DIR = pathlib.Path('/tmp/mct-test') / f'pid-{os.getpid()}'
os.environ.setdefault('SQLITE_DATA_DIR', str(TEST_DATA_DIR))
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.pop('REDIS_URL', None)

import fakeredis  # noqa: E402
from src.config import queue_client, database as dbmod  # noqa: E402

# 全局唯一 fakeredis（测试间 flush），database.ROUTER 在导入时已绑定它
queue_client.queue = fakeredis.FakeStrictRedis(decode_responses=True)
dbmod.queue = queue_client.queue
dbmod.ROUTER.q = queue_client.queue

from src.main import app  # noqa: E402
from src import worker as worker_mod  # noqa: E402
from src.config.database import (  # noqa: E402
    ENGINES, FACTORIES, ROUTER, Base, ReplicationJournal, SwitchOrder,
)
from scripts import seed_demo  # noqa: E402


@pytest.fixture()
def app_env():
    queue_client.queue.flushall()
    # 显式复位协调状态为初始主库，避免上一个用例切换后的指针残留拦截 seed 写入
    queue_client.queue.set(ROUTER.KEY_WRITER, 'primary')
    queue_client.queue.set(ROUTER.KEY_TERM, '1')
    queue_client.queue.set(ROUTER.KEY_PHASE, 'idle')
    queue_client.queue.set(ROUTER.KEY_LSN, '0')
    # 先释放连接池全部连接，再清双库重建，避免跨用例残留事务持锁
    for eng in ENGINES.values():
        eng.dispose()
    for eng in ENGINES.values():
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng)
    ROUTER._bootstrapped = True  # 状态已显式就绪，无需从空库引导
    # 复位故障注入与阶段钩子，避免跨用例污染
    from src.config.database import set_node_outage
    for n in ('primary', 'standby'):
        set_node_outage(n, False)
    from src.services import switchover as sw
    sw.phase_hook = None
    seed_demo.seed()
    yield {'db': dbmod, 'queue': queue_client.queue, 'app': app, 'worker': worker_mod}
    # 用例结束：清队列，防止残留切换作业污染下一个用例
    queue_client.queue.flushall()
    sw.phase_hook = None
    for n in ('primary', 'standby'):
        set_node_outage(n, False)


@pytest.fixture()
def client(app_env):
    from starlette.testclient import TestClient
    c = TestClient(app_env['app'])
    yield c
    c.close()


def _login(client, email):
    r = client.post('/auth/login', json={'email': email, 'password': 'Migrate2026!'})
    assert r.status_code == 200, r.text
    return {'Authorization': f"Bearer {r.json()['token']}"}


@pytest.fixture()
def admin_headers(client):
    return _login(client, 'admin@migration.local')


@pytest.fixture()
def operator_headers(client):
    return _login(client, 'operator@migration.local')


@pytest.fixture()
def viewer_headers(client):
    return _login(client, 'viewer@migration.local')


class WorkerThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            try:
                item = queue_client.queue.blpop(['migration-runs', 'switchover-jobs'], timeout=1)
                if not item:
                    continue
                name, value = item
                if name == 'migration-runs':
                    worker_mod.run_migration(int(value))
                else:
                    worker_mod.run_switch_job(value)
            except Exception:
                time.sleep(0.1)

    def stop(self):
        self._stop.set()


@pytest.fixture()
def worker():
    t = WorkerThread()
    t.start()
    yield t
    t.stop()


def wait_until(predicate, timeout=15, interval=0.1, label='condition'):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as e:  # noqa: BLE001
            last = repr(e)
        time.sleep(interval)
    raise AssertionError(f'等待超时: {label}; last={last!r}')
