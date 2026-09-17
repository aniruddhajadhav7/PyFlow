import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON, Uuid
from src.db import Base

class TaskLog(Base):
    __tablename__ = "task_logs"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_name = Column(String, index=True, nullable=False)
    queue_name = Column(String, index=True, nullable=False)
    status = Column(String, index=True, nullable=False)
    payload = Column(JSON, nullable=False)
    result = Column(JSON, nullable=True)
    error = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
