from src.config.database import ROUTER
from src.models.identity import Audit


def log(actor, action, detail=''):
    with ROUTER.writer_session() as s:
        s.add(Audit(actor=actor, action=action, detail=detail))
        s.commit()
