"""
Clerk JWT verification middleware for Aradune FastAPI backend.

Verifies Clerk session tokens on protected endpoints using Clerk's JWKS endpoint.
For now, only protects Intelligence/PolicyAnalyst endpoints.
All data endpoints remain public (they serve the free tier).

Usage:
    from server.middleware.auth import require_clerk_auth, get_clerk_user

    @router.post("/api/intelligence")
    async def intelligence(request: Request, user: dict = Depends(require_clerk_auth)):
        # user contains Clerk user claims (sub, email, etc.)
        ...

    # Or use optionally (don't block, just extract if present):
    @router.get("/api/some-endpoint")
    async def some_endpoint(request: Request, user: dict | None = Depends(get_clerk_user)):
        ...

Environment variables:
    CLERK_SECRET_KEY  - Clerk secret key (sk_live_... or sk_test_...)
                        When not set, auth is bypassed (development mode / password gate fallback)
"""

import os
import time
import json
import logging
from typing import Optional
from functools import lru_cache

from fastapi import Depends, HTTPException, Request

logger = logging.getLogger("aradune.auth")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLERK_SECRET_KEY = os.environ.get("CLERK_SECRET_KEY", "")
CLERK_ISSUER = os.environ.get("CLERK_ISSUER", "")  # e.g., https://clerk.your-domain.com

# When True, auth middleware is active. When False, all requests pass through.
AUTH_ENABLED = bool(CLERK_SECRET_KEY)

if AUTH_ENABLED:
    logger.info("Clerk auth enabled — protected endpoints require valid session tokens")
else:
    logger.info("Clerk auth disabled — all endpoints are open (password gate fallback)")


# ---------------------------------------------------------------------------
# JWT verification (lazy-loaded to avoid import errors when not configured)
# ---------------------------------------------------------------------------

_jwks_client = None
_jwt_module = None


def _get_jwt_module():
    """Lazy import PyJWT — only needed when Clerk auth is enabled."""
    global _jwt_module
    if _jwt_module is None:
        try:
            import jwt as pyjwt
            _jwt_module = pyjwt
        except ImportError:
            raise RuntimeError(
                "PyJWT is required for Clerk auth. Install it: pip install PyJWT[crypto]"
            )
    return _jwt_module


def _get_jwks_client():
    """Get or create the JWKS client for Clerk's public keys."""
    global _jwks_client
    if _jwks_client is None:
        jwt = _get_jwt_module()
        # Clerk publishes JWKS at the issuer URL + /.well-known/jwks.json
        # The issuer is derived from the Clerk frontend API domain
        # For Clerk, the JWKS URL can be constructed from the secret key
        # or set explicitly via CLERK_ISSUER
        if CLERK_ISSUER:
            jwks_url = f"{CLERK_ISSUER.rstrip('/')}/.well-known/jwks.json"
        else:
            # Extract instance ID from secret key to build JWKS URL
            # Clerk secret keys look like: sk_test_xxx or sk_live_xxx
            # The JWKS endpoint is at https://<clerk-frontend-api>/.well-known/jwks.json
            # Since we may not know the frontend API URL, we use the Clerk API
            # to fetch JWKS from https://api.clerk.com/v1/jwks
            jwks_url = "https://api.clerk.com/v1/jwks"

        _jwks_client = jwt.PyJWKClient(
            jwks_url,
            cache_keys=True,
            lifespan=3600,  # Cache keys for 1 hour
            headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"} if "api.clerk.com" in jwks_url else {},
        )
    return _jwks_client


def _verify_token(token: str) -> dict:
    """
    Verify a Clerk JWT and return the decoded claims.

    Returns dict with at least:
        - sub: Clerk user ID (user_xxx)
        - email: user email (if available in session claims)
        - iat, exp: issued at / expiration timestamps
    """
    jwt = _get_jwt_module()
    jwks_client = _get_jwks_client()

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={
                "verify_exp": True,
                "verify_iat": True,
                "require": ["sub", "iat", "exp"],
            },
            # Clerk tokens may or may not have an issuer/audience depending on config
            # We validate the signature and expiry, which is sufficient for auth
        )
        return decoded
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired — please sign in again")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid Clerk token: {e}")
        raise HTTPException(status_code=401, detail="Invalid session token")
    except Exception as e:
        logger.error(f"Token verification error: {e}")
        raise HTTPException(status_code=401, detail="Authentication error")


def _extract_token(request: Request) -> Optional[str]:
    """
    Extract the Clerk session token from the request.

    Checks (in order):
    1. Authorization: Bearer <token> header
    2. __session cookie (Clerk's default cookie name)
    """
    # Check Authorization header
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()

    # Check Clerk session cookie
    session_cookie = request.cookies.get("__session")
    if session_cookie:
        return session_cookie

    return None


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

async def require_clerk_auth(request: Request) -> dict:
    """
    FastAPI dependency — requires a valid Clerk session token.

    When CLERK_SECRET_KEY is not set (auth disabled), returns a stub user dict
    so endpoints continue to work without Clerk configured.

    Usage:
        @router.post("/endpoint")
        async def endpoint(user: dict = Depends(require_clerk_auth)):
            user_id = user["sub"]  # Clerk user ID
    """
    if not AUTH_ENABLED:
        # Auth not configured — return anonymous stub (password gate handles access)
        return {
            "sub": "anonymous",
            "auth_mode": "password_gate",
        }

    token = _extract_token(request)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authentication required — please sign in",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return _verify_token(token)


async def get_clerk_user(request: Request) -> Optional[dict]:
    """
    FastAPI dependency — optionally extracts Clerk user info.

    Returns None if no token is present (does NOT block the request).
    Useful for endpoints that work for both authenticated and anonymous users
    but want to personalize the response or log usage.
    """
    if not AUTH_ENABLED:
        return None

    token = _extract_token(request)
    if not token:
        return None

    try:
        return _verify_token(token)
    except HTTPException:
        return None
