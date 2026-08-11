import pytest
from src.rate_limiter import TokenBucketRateLimiter, SlidingWindowLogRateLimiter
import time
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_token_bucket_allowed(fake_redis):
    limiter = TokenBucketRateLimiter(fake_redis, limit=1, window=1)
    
    # We mock eval because fakeredis might not support our exact Lua script perfectly.
    # If fakeredis supports it, this works directly.
    fake_redis.eval = AsyncMock(return_value=1)
    
    allowed = await limiter.is_allowed("test_ip")
    assert allowed is True
    
    # Check that it called eval with right args
    fake_redis.eval.assert_called_once()
    args = fake_redis.eval.call_args[0]
    assert args[1] == 1 # num keys
    assert args[2] == "ratelimit:tb:test_ip"

@pytest.mark.asyncio
async def test_token_bucket_rejected(fake_redis):
    limiter = TokenBucketRateLimiter(fake_redis, limit=1, window=1)
    fake_redis.eval = AsyncMock(return_value=0)
    
    allowed = await limiter.is_allowed("test_ip")
    assert allowed is False

@pytest.mark.asyncio
async def test_sliding_window(fake_redis):
    limiter = SlidingWindowLogRateLimiter(fake_redis, limit=2, window=60)
    
    # Fakeredis handles pipelines and zsets fine, so we can test the real logic
    allowed = await limiter.is_allowed("sw_ip")
    assert allowed is True
    
    allowed = await limiter.is_allowed("sw_ip")
    assert allowed is True
    
    allowed = await limiter.is_allowed("sw_ip")
    assert allowed is False
