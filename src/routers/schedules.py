from fastapi import APIRouter, HTTPException, Depends, Request
from src.schemas.schedules import (
    ScheduleCreateRequest,
    ScheduleResponse,
    ScheduleListResponse,
    ScheduleMessageResponse,
)
from src.queue import RedisQueue
from uuid import UUID

router = APIRouter(prefix="/schedules", tags=["schedules"])

def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue

@router.post("/", response_model=ScheduleResponse, status_code=201)
async def create_schedule(request: ScheduleCreateRequest, queue: RedisQueue = Depends(get_queue)):
    """Create a new recurring scheduled task."""
    try:
        schedule_id = await queue.schedule_task(
            cron_expression=request.cron_expression,
            queue_name=request.queue_name,
            task_name=request.task_name,
            task_payload=request.payload
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    schedule_data = await queue.get_schedule(schedule_id)
    if not schedule_data:
        raise HTTPException(status_code=500, detail="Schedule was created but could not be retrieved.")
    return schedule_data

@router.get("/", response_model=ScheduleListResponse)
async def list_schedules(limit: int = 50, offset: int = 0, queue: RedisQueue = Depends(get_queue)):
    """List all scheduled tasks."""
    schedules = await queue.list_schedules(limit=limit, offset=offset)
    return {"schedules": schedules}

@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def retrieve_schedule(schedule_id: UUID, queue: RedisQueue = Depends(get_queue)):
    """Retrieve details of a specific schedule."""
    schedule_data = await queue.get_schedule(str(schedule_id))
    if not schedule_data:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    return schedule_data

@router.delete("/{schedule_id}", response_model=ScheduleMessageResponse)
async def cancel_schedule(schedule_id: UUID, queue: RedisQueue = Depends(get_queue)):
    """Cancel and delete a schedule."""
    success = await queue.cancel_schedule(str(schedule_id))
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Schedule could not be cancelled. It may not exist.",
        )
    return {"message": "Schedule cancelled successfully.", "success": True}
