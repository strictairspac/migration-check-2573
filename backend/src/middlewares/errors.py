import logging
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from src.config.database import WritePaused, FencingError, NodeUnavailable


def install_errors(app):
    @app.exception_handler(IntegrityError)
    async def conflict(request, exc):
        return JSONResponse({'detail': '名称已存在或关联数据不存在，请修改后重试'}, status_code=409)

    @app.exception_handler(WritePaused)
    async def write_paused(request, exc):
        # 423 Locked：交接 drain 短暂停写，客户端稍后重试同一单即可
        return JSONResponse({'detail': str(exc), 'retryable': True, 'code': 'write_paused'},
                            status_code=423)

    @app.exception_handler(FencingError)
    async def fenced(request, exc):
        # 409：旧主库任期栅栏，写入被永久拒绝；应在新主库重试
        return JSONResponse({'detail': str(exc), 'retryable': True, 'code': 'fenced'},
                            status_code=409)

    @app.exception_handler(NodeUnavailable)
    async def node_down(request, exc):
        return JSONResponse({'detail': str(exc), 'retryable': True, 'code': 'node_unavailable'},
                            status_code=503)

    @app.exception_handler(Exception)
    async def failure(request, exc):
        logging.exception('request failure')
        return JSONResponse({'detail': '服务暂时不可用，输入已保留，请稍后重试'}, status_code=500)
