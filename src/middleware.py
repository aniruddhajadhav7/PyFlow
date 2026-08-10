from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from src.rate_limiter import get_rate_limiter
from src.config import settings

class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # We only apply rate limiting to /tasks/ routes for this example
        if request.url.path.startswith("/tasks"):
            # Ensure redis_client is available on app state (or just reuse the queue's redis client)
            # Actually, app.state.queue.redis_client is available but wait, what if queue is not initialized yet?
            # It should be, since lifespan runs first.
            if hasattr(request.app.state, "queue"):
                redis_client = request.app.state.queue.redis_client
                
                # Use client IP as identifier (fallback to "unknown")
                client_ip = request.client.host if request.client else "unknown"
                
                limiter = get_rate_limiter(
                    redis_client=redis_client,
                    algorithm=settings.rate_limit_algorithm,
                    limit=settings.rate_limit_requests,
                    window=settings.rate_limit_window
                )
                
                is_allowed = await limiter.is_allowed(client_ip)
                
                if not is_allowed:
                    from fastapi.responses import JSONResponse
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "Rate limit exceeded. Please try again later."}
                    )
        
        response = await call_next(request)
        return response
