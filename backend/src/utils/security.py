import hashlib, hmac, secrets, jwt
from datetime import datetime, timezone, timedelta
from src.config.settings import JWT_SECRET


def hash_password(password):
    salt = secrets.token_hex(12)
    return salt + ':' + hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 120000).hex()


def verify_password(password, encoded):
    salt, digest = encoded.split(':')
    return hmac.compare_digest(hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 120000).hex(), digest)


def make_token(user_id, role):
    payload = {'sub': str(user_id), 'role': role,
               'exp': datetime.now(timezone.utc) + timedelta(hours=8)}
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def token(user):
    return make_token(user.id, user.role)
