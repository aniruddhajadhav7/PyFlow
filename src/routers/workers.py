from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List, Dict, Any
from src.queue import RedisQueue

router = APIRouter(prefix="/workers", tags=["workers"])


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


@router.get("/", response_model=Dict[str, Any])
@router.get("", response_model=Dict[str, Any])
async def list_workers(queue: RedisQueue = Depends(get_queue)):
    """
    List all active registered worker nodes, their hostnames, PIDs, subscribed queues, and current task loads.
    """
    workers = await queue.get_active_workers()
    total_capacity = sum(w["concurrency"] for w in workers)
    total_active = sum(w["active_tasks"] for w in workers)
    return {
        "count": len(workers),
        "total_concurrency": total_capacity,
        "total_active_tasks": total_active,
        "workers": workers,
    }


@router.get("/{worker_id}")
async def get_worker(worker_id: str, queue: RedisQueue = Depends(get_queue)):
    """
    Retrieve live health and workload metrics for a specific worker node.
    """
    workers = await queue.get_active_workers()
    for w in workers:
        if w["worker_id"] == worker_id:
            return w
    raise HTTPException(status_code=404, detail=f"Worker '{worker_id}' not found or inactive.")
