import pytest
import pytest_asyncio
import asyncio
from unittest.mock import patch, AsyncMock
from src.worker import Worker
from src.config import settings


@pytest_asyncio.fixture
async def worker(queue):
    w = Worker(redis_url=settings.redis_url, queue_name=queue.queue_name)
    w.queue = queue
    # We will override sleep to make tests fast
    yield w


@pytest.mark.asyncio
@patch("src.worker.asyncio.sleep", return_value=None)
async def test_worker_process_success(mock_sleep, worker, queue):
    task_id = await queue.enqueue({"data": "success_task"})

    # Dequeue manually and process
    task = await queue.dequeue()
    await worker._process_task(task)

    updated_task = await queue.get_task(task_id)
    assert updated_task["status"] == "SUCCESS"


@pytest.mark.asyncio
@patch("src.worker.asyncio.sleep", return_value=None)
async def test_worker_process_failure(mock_sleep, worker, queue):
    task_id = await queue.enqueue({"fail": True})

    task = await queue.dequeue()
    await worker._process_task(task)

    updated_task = await queue.get_task(task_id)
    assert updated_task["status"] == "PENDING"
    assert int(updated_task["retry_count"]) == 1
    assert "error" in updated_task
    assert "Simulated task failure." in updated_task["error"]


@pytest.mark.asyncio
async def test_worker_run_loop(worker, queue):
    # Mock dequeue to return a task once, then None
    task_data = {"id": "123", "payload": {"data": "loop_task"}, "status": "PENDING"}

    # We'll use a side_effect to return the task on first call, then None
    worker.queue.dequeue = AsyncMock(side_effect=[task_data, None, None, None, None])
    worker.queue.poll_delayed_tasks = AsyncMock()

    # Run the worker in a background task
    worker_task = asyncio.create_task(worker.run())

    # Let the loop run a bit
    await asyncio.sleep(0.1)

    # Trigger shutdown
    worker.shutdown_event.set()
    await worker_task

    # Check that dequeue was called
    assert worker.queue.dequeue.called
