import pytest
import pytest_asyncio
import fakeredis.aioredis
from httpx import AsyncClient, ASGITransport
from src.main import app
from src.queue import RedisQueue

@pytest_asyncio.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.close()

@pytest_asyncio.fixture
async def queue(fake_redis):
    q = RedisQueue(redis_url="redis://localhost:6379/0", queue_name="test_queue")
    q.redis_client = fake_redis
    yield q
    # We don't call q.close() because it would close the fake_redis we yielded

@pytest_asyncio.fixture
async def client(queue):
    app.state.queue = queue
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
