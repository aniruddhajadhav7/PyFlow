import pytest
from httpx import AsyncClient
from src.worker import Worker
from src.config import settings


@pytest.mark.asyncio
async def test_worker_heartbeat_and_discovery(queue):
    worker = Worker(redis_url=settings.redis_url, queues=["default", "high_priority"], max_concurrent_tasks=50)
    worker.queue = queue

    # Send heartbeat
    await worker._send_heartbeat(status="ACTIVE")

    # Discover via queue
    workers = await queue.get_active_workers()
    assert len(workers) == 1
    w = workers[0]
    assert w["worker_id"] == worker.worker_id
    assert w["hostname"] == worker.hostname
    assert w["pid"] == worker.pid
    assert w["concurrency"] == 50
    assert w["active_tasks"] == 0
    assert w["status"] == "ACTIVE"
    assert "default" in w["queues"]
    assert "high_priority" in w["queues"]
    assert w["uptime_seconds"] >= 0

    # Cleanup
    await worker._deregister()
    workers_after = await queue.get_active_workers()
    assert len(workers_after) == 0


@pytest.mark.asyncio
async def test_worker_api_endpoints(client: AsyncClient, queue):
    worker = Worker(redis_url=settings.redis_url, queues=["critical"], max_concurrent_tasks=25)
    worker.queue = queue

    await worker._send_heartbeat(status="ACTIVE")

    # GET /workers
    resp = await client.get("/workers")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 1
    assert data["total_concurrency"] >= 25
    found = any(w["worker_id"] == worker.worker_id for w in data["workers"])
    assert found is True

    # GET /workers/{worker_id}
    detail_resp = await client.get(f"/workers/{worker.worker_id}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["worker_id"] == worker.worker_id
    assert detail["concurrency"] == 25

    # GET non-existent worker
    missing_resp = await client.get("/workers/non-existent-worker-id")
    assert missing_resp.status_code == 404

    await worker._deregister()


@pytest.mark.asyncio
async def test_dashboard_stats_includes_workers(client: AsyncClient, queue):
    worker = Worker(redis_url=settings.redis_url, queues=["default"])
    worker.queue = queue
    await worker._send_heartbeat(status="ACTIVE")

    resp = await client.get("/dashboard/stats")
    assert resp.status_code == 200
    data = resp.json()

    assert "workers" in data
    assert isinstance(data["workers"], list)
    assert len(data["workers"]) >= 1
    assert "active_workers" in data["totals"]
    assert data["totals"]["active_workers"] >= 1
    assert "total_worker_capacity" in data["totals"]

    await worker._deregister()


@pytest.mark.asyncio
async def test_stale_worker_cleanup(queue):
    # Manually add a fake worker id with no corresponding hash data
    stale_id = "worker-stale-test-node-9999"
    await queue.redis_client.sadd("workers:active", stale_id)

    # Calling get_active_workers should detect missing hash and prune it from set
    workers = await queue.get_active_workers()
    assert all(w["worker_id"] != stale_id for w in workers)

    # Verify it was pruned from Redis set
    is_member = await queue.redis_client.sismember("workers:active", stale_id)
    assert not is_member
