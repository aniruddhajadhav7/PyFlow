from fastapi import FastAPI, Request, HTTPException
import structlog
from src.config import settings
from src.logger import setup_logging
from src.queue import RedisQueue
from src.routers import tasks
from src.middleware import RateLimitMiddleware
from contextlib import asynccontextmanager

setup_logging()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Application starting up", app_name=settings.app_name, debug=settings.debug
    )
    app.state.queue = RedisQueue(redis_url=settings.redis_url)
    yield
    logger.info("Application shutting down")
    await app.state.queue.close()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

app.add_middleware(RateLimitMiddleware)

app.include_router(tasks.router)


@app.get("/health")
async def health_check(request: Request):
    logger.info("Health check endpoint called")
    if hasattr(request.app.state, "queue"):
        try:
            await request.app.state.queue.redis_client.ping()
        except Exception:
            raise HTTPException(status_code=503, detail="Service Unavailable")
    return {"status": "ok", "app_name": settings.app_name}
