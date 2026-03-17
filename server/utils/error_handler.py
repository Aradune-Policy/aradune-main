"""
Shared error handling for all Aradune API routes.
Returns clean JSON error responses instead of 500 crashes.
"""

from fastapi.responses import JSONResponse
from functools import wraps
import logging
import traceback

logger = logging.getLogger("aradune.api")


def safe_route(default_response=None):
    """
    Decorator for route handlers. Catches all exceptions and returns
    a clean JSON response instead of a 500 error.

    Usage:
        @router.get("/api/pharmacy/summary")
        @safe_route(default_response={"states": [], "total": 0})
        async def pharmacy_summary(state: str = None):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                if result is None:
                    return default_response or {"data": [], "message": "No data available"}
                return result
            except Exception as e:
                logger.error(f"{func.__name__} failed: {e}\n{traceback.format_exc()}")
                return JSONResponse(
                    status_code=200,
                    content={
                        "data": [] if default_response is None else default_response,
                        "error": str(e)[:200],
                        "message": f"Data unavailable: {str(e)[:200]}"
                    }
                )
        return wrapper
    return decorator
