import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_dashboard_stats(client: AsyncClient, queue):
    # Enqueue a task to have some metrics
    await queue.enqueue("default", "test_stat_task", {"foo": "bar"})

    response = await client.get("/dashboard/stats")
    assert response.status_code == 200
    data = response.json()

    assert "queues" in data
    assert "default" in data["queues"]
    assert data["queues"]["default"]["pending"] >= 1
    assert "totals" in data
    assert data["totals"]["pending"] >= 1
    assert "redis" in data
    assert "schedules" in data
    assert "recent_history" in data


@pytest.mark.asyncio
async def test_dashboard_html_view(client: AsyncClient):
    response = await client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "PyFlow" in response.text
    assert "Task Queues" in response.text
