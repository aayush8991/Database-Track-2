from dotenv import load_dotenv
import os
import time
import json
import hmac
import base64
import hashlib
from typing import Dict

# Load environment variables early so users/API key are available at import time
load_dotenv()

# Lightweight HMAC-signed token utility (JWT-like, no external deps)
SECRET = os.getenv('DASHBOARD_SECRET', os.getenv('DASHBOARD_API_KEY', 'dev-dashboard-secret'))
TOKEN_EXP_SECONDS = int(os.getenv('DASHBOARD_TOKEN_EXP', '3600'))


def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip('=')


def _unb64u(s: str) -> bytes:
    padding = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def create_token(username: str, role: str = 'user', exp_seconds: int = None) -> str:
    if exp_seconds is None:
        exp_seconds = TOKEN_EXP_SECONDS
    payload = {'username': username, 'role': role, 'exp': int(time.time()) + int(exp_seconds)}
    p = json.dumps(payload, separators=(',', ':')).encode()
    pb = _b64u(p)
    sig = hmac.new(SECRET.encode(), pb.encode(), hashlib.sha256).digest()
    return pb + '.' + _b64u(sig)


def verify_token(token: str) -> Dict:
    try:
        pb, sb = token.split('.', 1)
        expected_sig = hmac.new(SECRET.encode(), pb.encode(), hashlib.sha256).digest()
        sig = _unb64u(sb)
        if not hmac.compare_digest(expected_sig, sig):
            raise ValueError('invalid signature')
        payload = json.loads(_unb64u(pb))
        if 'exp' in payload and int(payload['exp']) < int(time.time()):
            raise ValueError('token expired')
        return payload
    except Exception as e:
        raise ValueError('invalid token') from e


# Password hashing (PBKDF2)
def hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return dk.hex()


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    return hash_password(password, salt) == expected_hash


# Load users from env var DASHBOARD_USERS (format: user:pass:role,user2:pass2:role2)
def load_users() -> Dict[str, Dict]:
    users = {}
    raw = os.getenv('DASHBOARD_USERS')
    if raw:
        for item in raw.split(','):
            parts = item.split(':')
            if len(parts) >= 2:
                username = parts[0]
                password = parts[1]
                role = parts[2] if len(parts) > 2 else 'user'
                users[username] = {'password_hash': hash_password(password, username), 'role': role}

    # fallback single admin from DASHBOARD_ADMIN_PASS
    admin_pass = os.getenv('DASHBOARD_ADMIN_PASS')
    if admin_pass and 'admin' not in users:
        users['admin'] = {'password_hash': hash_password(admin_pass, 'admin'), 'role': 'admin'}

    # default dev user if nothing provided (username: admin, pass: admin)
    if not users:
        users['admin'] = {'password_hash': hash_password('admin', 'admin'), 'role': 'admin'}

    return users


USERS = load_users()
