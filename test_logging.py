import logging
import structlog

def setup_logging():
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        l = logging.getLogger(logger_name)
        l.handlers = [handler]
        l.propagate = False

setup_logging()
logging.getLogger("uvicorn.error").info("Test log")


