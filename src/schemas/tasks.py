from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from uuid import UUID

class TaskSubmitRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="The task data payload")

class TaskResponse(BaseModel):
    id: UUID = Field(..., description="The unique ID of the task")
    status: str = Field(..., description="The current status of the task")
    payload: Dict[str, Any] = Field(..., description="The task data payload")

class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]

class TaskMessageResponse(BaseModel):
    message: str
    success: bool
