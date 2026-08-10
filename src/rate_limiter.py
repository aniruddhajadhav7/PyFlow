import time
from typing import Optional
import redis.asyncio as redis

# Lua script for atomic Token Bucket operation
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- Calculate refill rate (tokens per second)
local rate = capacity / window

local last_tokens = tonumber(redis.call("hget", key, "tokens"))
if last_tokens == nil then
    last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("hget", key, "last_refreshed"))
if last_refreshed == nil then
    last_refreshed = now
end

local delta_time = math.max(0, now - last_refreshed)
local filled_tokens = math.min(capacity, last_tokens + (delta_time * rate))

local allowed = 0
if filled_tokens >= 1 then
    allowed = 1
    filled_tokens = filled_tokens - 1
end

redis.call("hset", key, "tokens", filled_tokens)
redis.call("hset", key, "last_refreshed", now)
redis.call("expire", key, math.ceil(window))

return allowed
"""

class BaseRateLimiter:
    def __init__(self, redis_client: redis.Redis, limit: int, window: int):
        self.redis = redis_client
        self.limit = limit
        self.window = window

    async def is_allowed(self, identifier: str) -> bool:
        raise NotImplementedError

class TokenBucketRateLimiter(BaseRateLimiter):
    async def is_allowed(self, identifier: str) -> bool:
        key = f"ratelimit:tb:{identifier}"
        now = time.time()
        
        # Execute Lua script
        allowed = await self.redis.eval(
            TOKEN_BUCKET_SCRIPT,
            1, # number of keys
            key,
            self.limit,
            self.window,
            now
        )
        return bool(allowed)

class SlidingWindowLogRateLimiter(BaseRateLimiter):
    async def is_allowed(self, identifier: str) -> bool:
        key = f"ratelimit:sw:{identifier}"
        now = time.time()
        window_start = now - self.window

        async with self.redis.pipeline(transaction=True) as pipe:
            # Remove old entries
            pipe.zremrangebyscore(key, 0, window_start)
            # Add current request
            pipe.zadd(key, {str(now): now})
            # Count requests in window
            pipe.zcard(key)
            # Set expiry so we don't leak memory
            pipe.expire(key, self.window)
            
            results = await pipe.execute()
            
            # The count is the 3rd result (index 2)
            request_count = results[2]
            
            return request_count <= self.limit

def get_rate_limiter(redis_client: redis.Redis, algorithm: str, limit: int, window: int) -> BaseRateLimiter:
    if algorithm == "sliding_window":
        return SlidingWindowLogRateLimiter(redis_client, limit, window)
    return TokenBucketRateLimiter(redis_client, limit, window)
