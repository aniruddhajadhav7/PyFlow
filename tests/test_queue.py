import pytest


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(queue):
    payload = {"task": "do_something"}
    task_id = await queue.enqueue(payload)

    assert task_id is not None

    length = await queue.queue_length()
    assert length == 1

    task = await queue.dequeue()
    assert task is not None
    assert task["id"] == task_id
    assert task["payload"] == payload
    assert task["status"] == "RUNNING"

    length = await queue.queue_length()
    assert length == 0


@pytest.mark.asyncio
async def test_peek(queue):
    payload = {"task": "test_peek"}
    task_id = await queue.enqueue(payload)

    task = await queue.peek()
    assert task is not None
    assert task["id"] == task_id
    # status should still be PENDING
    assert task["status"] == "PENDING"

    length = await queue.queue_length()
    assert length == 1


@pytest.mark.asyncio
async def test_cancel_task(queue):
    task_id = await queue.enqueue({"data": "cancel_me"})

    success = await queue.cancel_task(task_id)
    assert success is True

    task = await queue.get_task(task_id)
    assert task["status"] == "FAILED"

    length = await queue.queue_length()
    assert length == 0


@pytest.mark.asyncio
async def test_fail_task_with_retry(queue):
    task_id = await queue.enqueue({"data": "fail_me"})
    await queue.dequeue()  # Make it RUNNING

    await queue.fail_task(task_id, "Test error", max_retries=3, base_delay=0)

    task = await queue.get_task(task_id)
    assert task["status"] == "PENDING"
    assert int(task["retry_count"]) == 1

    # Needs to poll delayed tasks to put back in main queue
    await queue.poll_delayed_tasks()

    length = await queue.queue_length()
    assert length == 1


@pytest.mark.asyncio
async def test_fail_task_max_retries(queue):
    task_id = await queue.enqueue({"data": "fail_me_max"})
    await queue.dequeue()

    await queue.fail_task(task_id, "Test error", max_retries=0, base_delay=0)

    task = await queue.get_task(task_id)
    assert task["status"] == "FAILED"

    length = await queue.queue_length()
    assert length == 0


@pytest.mark.asyncio
async def test_retry_task(queue):
    task_id = await queue.enqueue({"data": "manual_retry"})
    await queue.dequeue()
    await queue.fail_task(task_id, "Test error", max_retries=0, base_delay=0)

    success = await queue.retry_task(task_id)
    assert success is True

    task = await queue.get_task(task_id)
    assert task["status"] == "PENDING"
    assert int(task["retry_count"]) == 0

    length = await queue.queue_length()
    assert length == 1
