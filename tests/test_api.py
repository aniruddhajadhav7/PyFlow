import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_task(client: AsyncClient):
    payload = {"task": "from_api"}
    response = await client.post("/tasks/", json={"payload": payload})

    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["payload"] == payload
    assert data["status"] == "PENDING"


@pytest.mark.asyncio
async def test_list_tasks(client: AsyncClient):
    # submit 2 tasks
    await client.post("/tasks/", json={"payload": {"task": 1}})
    await client.post("/tasks/", json={"payload": {"task": 2}})

    response = await client.get("/tasks/")
    assert response.status_code == 200
    data = response.json()
    assert "tasks" in data
    assert len(data["tasks"]) >= 2


@pytest.mark.asyncio
async def test_retrieve_task(client: AsyncClient):
    submit_resp = await client.post("/tasks/", json={"payload": {"task": "retrieve"}})
    task_id = submit_resp.json()["id"]

    response = await client.get(f"/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == task_id
    assert data["payload"] == {"task": "retrieve"}


@pytest.mark.asyncio
async def test_cancel_task(client: AsyncClient):
    submit_resp = await client.post("/tasks/", json={"payload": {"task": "cancel"}})
    task_id = submit_resp.json()["id"]

    response = await client.delete(f"/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


@pytest.mark.asyncio
async def test_retry_task(client: AsyncClient, queue):
    # Enqueue a task manually and fail it so we can retry via API
    task_id = await queue.enqueue({"task": "retry"})
    await queue.dequeue()
    await queue.fail_task(task_id, "error", max_retries=0)

    response = await client.post(f"/tasks/{task_id}/retry")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True

    # check status
    task = await queue.get_task(task_id)
    assert task["status"] == "PENDING"


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient, queue):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    
    # Mock redis ping failure
    from unittest.mock import AsyncMock
    queue.redis_client.ping = AsyncMock(side_effect=Exception("Redis down"))
    response = await client.get("/health")
    assert response.status_code == 503
