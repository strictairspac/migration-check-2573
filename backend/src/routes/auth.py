from fastapi import APIRouter, HTTPException, Depends
from src.config.database import ROUTER
from src.models.identity import User
from src.schemas.business import Login
from src.utils.security import make_token, verify_password
from src.middlewares.auth import current
from src.services.audit import log
router = APIRouter()


@router.post('/auth/login')
def login(data: Login):
    _node, s = ROUTER.reader_session()
    with s:
        u = s.query(User).filter_by(email=data.email).first()
        if not u or not verify_password(data.password, u.password):
            raise HTTPException(401, '邮箱或密码不正确')
        user_id, email, role = u.id, u.email, u.role
    log(email, 'auth.login')
    return {'token': make_token(user_id, role), 'user': {'email': email, 'role': role}}


@router.get('/auth/me')
def me(u=Depends(current)):
    return {'email': u.email, 'role': u.role}
