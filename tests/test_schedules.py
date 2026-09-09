import pytest
import asyncio
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_create_schedule(client: AsyncClient, queue):
    payload = {"task": "do_cron"}
    response = await client.post(
        "/schedules/",
        json={
            "cron_expression": "* * * * *",
            "queue_name": "default",
            "task_name": "test_cron",
            "payload": payload
        }
    )

    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["cron_expression"] == "* * * * *"
    assert data["queue_name"] == "default"
    assert data["task_name"] == "test_cron"
    assert data["payload"] == payload
    assert "next_execution" in data

    # Verify it exists in DB
    schedule = await queue.get_schedule(data["id"])
    assert schedule is not None


@pytest.mark.asyncio
async def test_invalid_cron(client: AsyncClient):
    response = await client.post(
        "/schedules/",
        json={
            "cron_expression": "invalid_cron",
            "queue_name": "default",
            "task_name": "test_cron"
        }
    )
    assert response.status_code == 422  # validation error from pydantic


@pytest.mark.asyncio
async def test_list_schedules(client: AsyncClient):
    await client.post("/schedules/", json={"cron_expression": "*/5 * * * *", "task_name": "t1"})
    await client.post("/schedules/", json={"cron_expression": "0 * * * *", "task_name": "t2"})

    response = await client.get("/schedules/")
    assert response.status_code == 200
    data = response.json()
    assert "schedules" in data
    assert len(data["schedules"]) >= 2


@pytest.mark.asyncio
async def test_retrieve_schedule(client: AsyncClient):
    submit_resp = await client.post("/schedules/", json={"cron_expression": "*/2 * * * *", "task_name": "t3"})
    schedule_id = submit_resp.json()["id"]

    response = await client.get(f"/schedules/{schedule_id}")
    assert response.status_code == 200
    assert response.json()["id"] == schedule_id


@pytest.mark.asyncio
async def test_cancel_schedule(client: AsyncClient):
    submit_resp = await client.post("/schedules/", json={"cron_expression": "*/2 * * * *", "task_name": "t4"})
    schedule_id = submit_resp.json()["id"]

    response = await client.delete(f"/schedules/{schedule_id}")
    assert response.status_code == 200
    
    get_resp = await client.get(f"/schedules/{schedule_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_poll_scheduled_tasks(queue):
    import time
    # Create a schedule manually with a past time
    schedule_id = await queue.schedule_task("* * * * *", "default", "cron_test", {"id": 1})
    
    # Manipulate the execution time to be in the past to trigger it
    now = time.time()
    await queue.redis_client.zadd("schedules:execution", {schedule_id: now - 100})
    
    # Poll
    await queue.poll_scheduled_tasks()
    
    # Verify a task was enqueued
    q_len = await queue.queue_length("default")
    assert q_len == 1
    
    # Verify schedule's next execution is updated
    score = await queue.redis_client.zscore("schedules:execution", schedule_id)
    assert score > now
