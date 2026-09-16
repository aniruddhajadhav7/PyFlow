from pydantic import BaseModel, Field, validator
from typing import Dict, Any, List
from uuid import UUID
from croniter import croniter

class ScheduleCreateRequest(BaseModel):
    cron_expression: str = Field(..., description="Cron expression for the schedule")
    queue_name: str = Field("default", description="The queue to route the task to")
    task_name: str = Field(..., description="The name or type of the task")
    payload: Dict[str, Any] = Field(default_factory=dict, description="The task data payload")

    @validator('cron_expression')
    def validate_cron(cls, v):
        if not croniter.is_valid(v):
            raise ValueError('Invalid cron expression')
        return v


class ScheduleResponse(BaseModel):
    id: UUID = Field(..., description="The unique ID of the schedule")
    cron_expression: str = Field(..., description="Cron expression for the schedule")
    queue_name: str = Field(..., description="The queue to route the task to")
    task_name: str = Field(..., description="The name or type of the task")
    payload: Dict[str, Any] = Field(..., description="The task data payload")
    next_execution: float = Field(..., description="Unix timestamp of the next execution")
    created_at: float = Field(..., description="Unix timestamp of creation")


class ScheduleListResponse(BaseModel):
    schedules: List[ScheduleResponse]


class ScheduleMessageResponse(BaseModel):
    message: str
    success: bool
