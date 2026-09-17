import pytest_asyncio
import fakeredis.aioredis
from httpx import AsyncClient, ASGITransport
from src.main import app
from src.queue import RedisQueue
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool
from src.db import Base, get_db

# Create an in-memory SQLite engine for tests
test_engine = create_async_engine(
    "sqlite+aiosqlite:///:memory:", 
    echo=False, 
    poolclass=StaticPool,
    connect_args={"check_same_thread": False}
)
TestingSessionLocal = async_sessionmaker(
    bind=test_engine, class_=AsyncSession, expire_on_commit=False
)

@pytest_asyncio.fixture(autouse=True)
async def setup_test_db(monkeypatch):
    import src.db
    monkeypatch.setattr(src.db, "AsyncSessionLocal", TestingSessionLocal)
    monkeypatch.setattr(src.db, "engine", test_engine)
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

async def override_get_db():
    async with TestingSessionLocal() as session:
        yield session

app.dependency_overrides[get_db] = override_get_db

@pytest_asyncio.fixture
async def db_session():
    async with TestingSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def queue(fake_redis):
    q = RedisQueue(redis_url="redis://localhost:6379/0")
    q.redis_client = fake_redis
    yield q


@pytest_asyncio.fixture
async def client(queue):
    app.state.queue = queue
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
