import os
import hmac
import hashlib
import secrets

def generate_session_token(secret: str = None) -> tuple[str, str]:
    session_id = f"sess_{secrets.token_hex(4)}"
    if not secret:
        secret = os.getenv("SESSION_SECRET", "default_secret")
    signature = hmac.new(secret.encode(), session_id.encode(), hashlib.sha256).hexdigest()
    return session_id, signature

def verify_session_token(session_id: str, signature: str, secret: str = None) -> bool:
    if not secret:
        secret = os.getenv("SESSION_SECRET", "default_secret")
    expected = hmac.new(secret.encode(), session_id.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
