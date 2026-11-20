import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from src.config.database import init_db, ROUTER, NodeUnavailable, WritePaused, FencingError
from src.config.queue_client import queue
from src.routes import auth, batches, mappings, runs, audit, switch as switch_routes
from src.middlewares.errors import install_errors
from src.middlewares.requests import RequestTrace


@asynccontextmanager
async def lifespan(app):
    init_db(queue)
    if os.getenv('SEED_DEMO', 'true').lower() == 'true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:
            logging.exception('seed failed; API remains available')
    worker_thread = None
    if os.getenv('EMBED_WORKER', 'false').lower() == 'true':
        import threading
        from src.worker import main as worker_main
        worker_thread = threading.Thread(target=worker_main, daemon=True, name='embedded-worker')
        worker_thread.start()
    yield


app = FastAPI(title='见山 · 检验结果迁移核对站', lifespan=lifespan)
app.add_middleware(RequestTrace)
install_errors(app)
for r in (auth, batches, mappings, runs, audit, switch_routes):
    app.include_router(r.router)


@app.get('/health')
def health():
    snap = ROUTER.snapshot()
    queue.ping()
    return {'status': 'ok', 'writer_node': snap['writer_node'],
            'reader_node': snap['reader_node'], 'phase': snap['phase']}
