"""单消费者后台 worker：迁移任务 + 主副库切换任务。

- 迁移提交撞上 drain（WritePaused）或任期翻转（FencingError）时按幂等重试，
  最终在新主库一次提交，不产生重复目标行。
- 切换任务在节点中断时由 switchover 服务重新入队，恢复后从断点续跑。
"""
import time
import logging
from pathlib import Path

from src.config.database import (
    ROUTER, FACTORIES, init_db, WritePaused, FencingError, NodeUnavailable,
)
from src.config.queue_client import queue
from src.models.run import Run, TargetRow
from src.services.migration import migrate, make_backup, key_digest, Cancelled
from src.services.audit import log
from src.services import switchover
from src.services.runs import RUN_QUEUE

HEARTBEAT = Path('/tmp/worker-heartbeat')
MAX_RETRY = 40
RETRY_BASE = 0.05


def heartbeat():
    HEARTBEAT.touch()


def progress(run_id, info):
    heartbeat()
    with ROUTER.writer_session() as s:
        r = s.get(Run, run_id)
        if r.cancel_requested or r.status == 'cancelled':
            raise Cancelled()
        r.progress = info
        s.commit()


def run_migration(run_id):
    with FACTORIES[ROUTER.writer_node()]() as s:
        r = s.query(Run).filter_by(id=run_id).with_for_update().first()
        if not r or r.status != 'queued':
            return
        r.status = 'running'
        snapshot = r.snapshot
        s.commit()
    try:
        result = migrate(snapshot['batch']['rows'], snapshot['mapping']['rules'],
                         snapshot['mapping']['chunk_size'], lambda info: progress(run_id, info))
        result['backup'] = make_backup(result, run_id, snapshot['batch']['name'])
        commit_with_retry(run_id, result)
        log('worker', 'run.completed', str(run_id))
    except Cancelled:
        with ROUTER.writer_session() as s:
            r = s.get(Run, run_id)
            r.status = 'cancelled'
            r.error = '用户取消，未提交目标行'
            s.commit()
        log('worker', 'run.cancelled', str(run_id))
    except Exception as exc:
        logging.exception('migration failed')
        with ROUTER.writer_session() as s:
            r = s.get(Run, run_id)
            if r and r.status == 'running':
                r.status = 'failed'
                r.error = str(exc)[:1000]
                s.commit()


def commit_with_retry(run_id, result):
    """目标行提交：drain 停写/任期翻转时退避重试，始终在当前主库一次提交。"""
    attempt = 0
    while True:
        try:
            with ROUTER.writer_session() as s:
                r = s.query(Run).filter_by(id=run_id).with_for_update().first()
                if r is None:
                    return
                if r.cancel_requested:
                    raise Cancelled()
                if r.status == 'completed':
                    return  # 重试同一单不重复落库
                # 清理任何半成品行（正常路径没有，这里保证幂等）
                s.query(TargetRow).filter_by(run_id=run_id).delete()
                for i, row in enumerate(result['accepted_rows']):
                    s.add(TargetRow(run_id=run_id, ordinal=i,
                                    key_digest=key_digest(row), payload=row))
                r.result = result
                r.status = 'completed'
                s.commit()
            return
        except (WritePaused, FencingError, NodeUnavailable) as e:
            attempt += 1
            if attempt > MAX_RETRY:
                raise RuntimeError(f'交接窗口内多次重试仍未提交：{e}')
            time.sleep(RETRY_BASE * attempt)


def run_switch_job(order_id):
    try:
        switchover.run_switch(int(order_id))
    except NodeUnavailable:
        logging.warning('switch #%s paused: node unavailable, requeued', order_id)
    except Exception:
        logging.exception('switch job failed')


def main():
    init_db(queue)
    logging.info('worker started, writer=%s', ROUTER.writer_node())
    while True:
        heartbeat()
        try:
            # 迁移队列优先；切换队列随后，均用短超时保持心跳
            item = queue.blpop([RUN_QUEUE, ROUTER.JOB_QUEUE], timeout=2)
            if not item:
                continue
            name, value = item
            if name == RUN_QUEUE:
                run_migration(int(value))
            else:
                run_switch_job(value)
        except Exception:
            logging.exception('worker loop')
            time.sleep(2)


if __name__ == '__main__':
    main()
