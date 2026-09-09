import pytest


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(queue):
    payload = {"task": "do_something"}
    task_id = await queue.enqueue("default", "test_task", payload)

    assert task_id is not None

    length = await queue.queue_length("default")
    assert length == 1

    task = await queue.dequeue(["default"])
    assert task is not None
    assert task["id"] == task_id
    assert task["payload"] == payload
    assert task["task_name"] == "test_task"
    assert task["status"] == "RUNNING"

    length = await queue.queue_length("default")
    assert length == 0


@pytest.mark.asyncio
async def test_peek(queue):
    payload = {"task": "test_peek"}
    task_id = await queue.enqueue("default", "test_peek", payload)

    task = await queue.peek("default")
    assert task is not None
    assert task["id"] == task_id
    # status should still be PENDING
    assert task["task_name"] == "test_peek"
    assert task["status"] == "PENDING"

    length = await queue.queue_length("default")
    assert length == 1


@pytest.mark.asyncio
async def test_cancel_task(queue):
    task_id = await queue.enqueue("default", "cancel_task", {"data": "cancel_me"})

    success = await queue.cancel_task(task_id)
    assert success is True

    task = await queue.get_task(task_id)
    assert task["status"] == "FAILED"

    length = await queue.queue_length("default")
    assert length == 0


@pytest.mark.asyncio
async def test_fail_task_with_retry(queue):
    task_id = await queue.enqueue("default", "fail_task", {"data": "fail_me"})
    await queue.dequeue(["default"])  # Make it RUNNING

    await queue.fail_task(task_id, "Test error", max_retries=3, base_delay=0)

    task = await queue.get_task(task_id)
    assert task["status"] == "PENDING"
    assert int(task["retry_count"]) == 1

    # Needs to poll delayed tasks to put back in main queue
    await queue.poll_delayed_tasks(["default"])

    length = await queue.queue_length("default")
    assert length == 1


@pytest.mark.asyncio
async def test_fail_task_max_retries(queue):
    task_id = await queue.enqueue("default", "fail_task_max", {"data": "fail_me_max"})
    await queue.dequeue(["default"])

    await queue.fail_task(task_id, "Test error", max_retries=0, base_delay=0)

    task = await queue.get_task(task_id)
    assert task["status"] == "FAILED"

    length = await queue.queue_length("default")
    assert length == 0


@pytest.mark.asyncio
async def test_retry_task(queue):
    task_id = await queue.enqueue("default", "retry_task", {"data": "manual_retry"})
    await queue.dequeue(["default"])
    await queue.fail_task(task_id, "Test error", max_retries=0, base_delay=0)

    success = await queue.retry_task(task_id)
    assert success is True

    task = await queue.get_task(task_id)
    assert task["status"] == "PENDING"
    assert int(task["retry_count"]) == 0

    length = await queue.queue_length("default")
    assert length == 1


@pytest.mark.asyncio
async def test_list_tasks_pagination(queue):
    import asyncio
    # Enqueue multiple tasks
    for i in range(5):
        await queue.enqueue("default", f"task_{i}", {"task": i})
        await asyncio.sleep(0.01)  # ensure different timestamps

    tasks_page_1 = await queue.list_tasks(limit=2, offset=0)
    assert len(tasks_page_1) == 2
    
    tasks_page_2 = await queue.list_tasks(limit=2, offset=2)
    assert len(tasks_page_2) == 2
    assert tasks_page_1[0]["id"] != tasks_page_2[0]["id"]
    
    tasks_page_3 = await queue.list_tasks(limit=2, offset=4)
    assert len(tasks_page_3) == 1
