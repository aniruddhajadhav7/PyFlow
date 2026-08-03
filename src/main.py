from fastapi import FastAPI
import structlog
from src.config import settings
from src.logger import setup_logging

setup_logging()
logger = structlog.get_logger(__name__)

app = FastAPI(title=settings.app_name, debug=settings.debug)

@app.on_event("startup")
async def startup_event():
    logger.info("Application starting up", app_name=settings.app_name, debug=settings.debug)

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint called")
    return {"status": "ok", "app_name": settings.app_name}
