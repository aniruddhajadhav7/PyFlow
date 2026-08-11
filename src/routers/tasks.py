from fastapi import APIRouter, HTTPException, Depends, Request
from src.schemas.tasks import (
    TaskSubmitRequest,
    TaskResponse,
    TaskListResponse,
    TaskMessageResponse,
)
from src.queue import RedisQueue
from uuid import UUID

router = APIRouter(prefix="/tasks", tags=["tasks"])


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


@router.post("/", response_model=TaskResponse)
async def submit_task(
    request: TaskSubmitRequest, queue: RedisQueue = Depends(get_queue)
):
    """Submit a new task to the queue."""
    task_id = await queue.enqueue(request.payload)
    task_data = await queue.get_task(task_id)
    if not task_data:
        raise HTTPException(
            status_code=500, detail="Task was enqueued but could not be retrieved."
        )
    return task_data


@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    limit: int = 50, offset: int = 0, queue: RedisQueue = Depends(get_queue)
):
    """List all tasks."""
    tasks = await queue.list_tasks(limit=limit, offset=offset)
    return {"tasks": tasks}


@router.get("/{task_id}", response_model=TaskResponse)
async def retrieve_task(task_id: UUID, queue: RedisQueue = Depends(get_queue)):
    """Retrieve the status and payload of a specific task."""
    task_data = await queue.get_task(str(task_id))
    if not task_data:
        raise HTTPException(status_code=404, detail="Task not found.")
    return task_data


@router.post("/{task_id}/cancel", response_model=TaskMessageResponse)
async def cancel_task(task_id: UUID, queue: RedisQueue = Depends(get_queue)):
    """Cancel a pending task."""
    success = await queue.cancel_task(str(task_id))
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Task could not be cancelled. It may not exist or is no longer PENDING.",
        )
    return {"message": "Task cancelled successfully.", "success": True}


@router.post("/{task_id}/retry", response_model=TaskMessageResponse)
async def retry_task(task_id: UUID, queue: RedisQueue = Depends(get_queue)):
    """Retry a failed task."""
    success = await queue.retry_task(str(task_id))
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Task could not be retried. It may not exist or is not FAILED.",
        )
    return {"message": "Task requeued for retry successfully.", "success": True}
