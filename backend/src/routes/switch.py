from fastapi import APIRouter, Depends
from src.middlewares.auth import current, admin
from src.services import switchover
from src.services.audit import log
from src.config.database import set_node_outage, ROUTER, node_in_outage

router = APIRouter()


@router.get('/switch/status')
def status(u=Depends(current)):
    return switchover.status_snapshot()


@router.get('/switch/orders')
def orders(u=Depends(admin)):
    return switchover.list_orders()


@router.post('/switch/initiate')
def initiate(u=Depends(admin)):
    row = switchover.initiate(u.email)
    return row


# ---- 故障注入：仅本地演练/验收，生产环境网关层屏蔽 --------------------------------
@router.post('/switch/fault/node')
def fault(node: str, down: bool, u=Depends(admin)):
    if node not in ('primary', 'standby'):
        from fastapi import HTTPException
        raise HTTPException(422, 'node 仅支持 primary/standby')
    set_node_outage(node, down)
    log(u.email, 'switch.fault.inject', f'{node} down={down}')
    return {'node': node, 'outage': node_in_outage(node),
            'healthy': {n: ROUTER.node_healthy(n) for n in ('primary', 'standby')}}
