"""Authentication & RBAC.

Elevated access is granted only through a signed login token: POST /api/auth/login
verifies the user's credentials against MongoDB and mints an HMAC-signed token
carrying the caller's role. Every request presents that token as
`Authorization: Bearer <token>`; requests without a valid token resolve to the
read-only public tier (fail-closed). Passwords are stored as salted PBKDF2
hashes — never plaintext.
"""
import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Optional

from fastapi import Header, HTTPException, status

from backend.config import settings

ROLE_MOSPI_REVIEWER = "MoSPI Reviewer"
ROLE_DISTRICT_AUDITOR = "District Authority Auditor"
ROLE_PUBLIC_TIER = "Read-Only Public Tier"

VALID_ROLES = {ROLE_MOSPI_REVIEWER, ROLE_DISTRICT_AUDITOR, ROLE_PUBLIC_TIER}
ELEVATED_ROLES = {ROLE_MOSPI_REVIEWER, ROLE_DISTRICT_AUDITOR}

# Fail-closed: any missing/invalid/expired token degrades to the read-only
# public tier. Elevated access must come from a signed login token.
DEFAULT_ROLE = ROLE_PUBLIC_TIER

_TOKEN_TTL_SECONDS = 7 * 24 * 3600
_PBKDF2_ITERATIONS = 200_000


# ------------------------------------------------------------------------------
# Password hashing (PBKDF2-HMAC-SHA256, stdlib only)
# ------------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("ascii"), _PBKDF2_ITERATIONS
    )
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt}${digest.hex()}"


def verify_password(password: str, user: dict) -> bool:
    """Check a candidate password against a user document. Accepts the salted
    hash and — for rows not yet migrated — the legacy plaintext field."""
    stored_hash = user.get("password_hash")
    if stored_hash:
        try:
            scheme, iterations, salt, digest = str(stored_hash).split("$")
            candidate = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt.encode("ascii"), int(iterations)
            )
        except (ValueError, TypeError):
            return False
        if scheme != "pbkdf2_sha256":
            return False
        return hmac.compare_digest(candidate.hex(), str(digest))
    legacy = user.get("password")
    return isinstance(legacy, str) and hmac.compare_digest(legacy, password)


# ------------------------------------------------------------------------------
# Signed login tokens (HMAC-SHA256 over a base64url JSON payload)
# ------------------------------------------------------------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def _sign(payload: str) -> str:
    key = settings.AUTH_SECRET.encode("utf-8")
    return _b64url(hmac.new(key, payload.encode("ascii"), hashlib.sha256).digest())


def mint_token(username: str, role: str) -> str:
    payload = _b64url(json.dumps(
        {"u": username, "r": role, "e": int(time.time()) + _TOKEN_TTL_SECONDS},
        separators=(",", ":"),
    ).encode("utf-8"))
    return f"{payload}.{_sign(payload)}"


def verify_token(token: str) -> Optional[dict]:
    """Return the token claims when the signature and expiry are valid, else None."""
    if not token or "." not in token:
        return None
    payload, _, signature = token.partition(".")
    if not hmac.compare_digest(signature, _sign(payload)):
        return None
    try:
        claims = json.loads(_b64url_decode(payload))
    except ValueError:
        return None
    if not isinstance(claims, dict) or int(claims.get("e", 0)) < time.time():
        return None
    return claims


# ------------------------------------------------------------------------------
# FastAPI dependencies
# ------------------------------------------------------------------------------

def _role_from_credentials(authorization: Optional[str]) -> str:
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer":
            claims = verify_token(token.strip())
            if claims and claims.get("r") in VALID_ROLES:
                return claims["r"]
    return DEFAULT_ROLE


def get_current_role(
    authorization: Optional[str] = Header(
        default=None,
        description="Signed login token: 'Authorization: Bearer <token>' (minted by POST /api/auth/login)"
    )
) -> str:
    return _role_from_credentials(authorization)


def require_reviewer_role(
    authorization: Optional[str] = Header(default=None)
) -> str:
    role = _role_from_credentials(authorization)
    if role not in ELEVATED_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: Read-Only Public Tier cannot record or alter human "
                   "review outcomes. Sign in via POST /api/auth/login."
        )
    return role


def require_mospi_admin_role(
    authorization: Optional[str] = Header(default=None)
) -> str:
    role = _role_from_credentials(authorization)
    if role != ROLE_MOSPI_REVIEWER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: Only MoSPI Reviewers can perform governance and sync "
                   "operations. Sign in via POST /api/auth/login."
        )
    return role
