from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from uuid import UUID


class TaskSubmitRequest(BaseModel):
    task_name: str = Field(..., description="The name or type of the task")
    payload: Dict[str, Any] = Field(default_factory=dict, description="The task data payload")
    queue_name: str = Field("default", description="The queue to route the task to")
    timeout: Optional[float] = Field(None, gt=0, description="Timeout in seconds for this task execution")


class TaskResponse(BaseModel):
    id: UUID = Field(..., description="The unique ID of the task")
    queue_name: str = Field(..., description="The queue this task belongs to")
    task_name: str = Field(..., description="The name or type of the task")
    status: str = Field(..., description="The current status of the task")
    payload: Dict[str, Any] = Field(..., description="The task data payload")
    result: Optional[Any] = Field(None, description="The result of the task if completed successfully")
    error: Optional[str] = Field(None, description="The error message if the task failed")
    timeout: Optional[float] = Field(None, description="Configured timeout in seconds for this task")


class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]


class TaskMessageResponse(BaseModel):
    message: str
    success: bool
