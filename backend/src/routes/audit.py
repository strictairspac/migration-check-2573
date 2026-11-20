from fastapi import APIRouter, Depends
from src.middlewares.auth import admin
from src.config.database import ROUTER
from src.models.identity import Audit
from src.utils.serialization import record
router = APIRouter()


@router.get('/audit')
def audit(u=Depends(admin)):
    _node, s = ROUTER.reader_session()
    with s:
        return [record(r) for r in s.query(Audit).order_by(Audit.id.desc()).limit(100)]
