import json
import uuid
from typing import Any, Dict, Optional
import redis.asyncio as redis

class QueueError(Exception):
    """Base class for queue-related exceptions."""
    pass

class TaskNotFoundError(QueueError):
    """Raised when a task is not found in the storage."""
    pass

class RedisQueue:
    def __init__(self, redis_url: str, queue_name: str = "default_queue"):
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.queue_key = f"queue:{self.queue_name}"

    async def enqueue(self, task_payload: dict) -> str:
        """
        Enqueues a task and returns its unique task_id.
        """
        task_id = str(uuid.uuid4())
        task_key = f"task:{task_id}"
        
        # Serialize the payload
        try:
            serialized_payload = json.dumps(task_payload)
        except (TypeError, ValueError) as e:
            raise QueueError(f"Failed to serialize task payload: {e}")

        task_data = {
            "id": task_id,
            "payload": serialized_payload,
            "status": "pending"
        }

        try:
            # Store task data in Hash
            await self.redis_client.hset(task_key, mapping=task_data)
            # Push task ID to the list
            await self.redis_client.rpush(self.queue_key, task_id)
            return task_id
        except redis.RedisError as e:
            raise QueueError(f"Redis error during enqueue: {e}")

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Dequeues a task from the front of the queue, updates its status, and returns its data.
        Returns None if the queue is empty.
        """
        try:
            task_id = await self.redis_client.lpop(self.queue_key)
            if not task_id:
                return None

            task_key = f"task:{task_id}"
            task_data = await self.redis_client.hgetall(task_key)
            
            if not task_data:
                raise TaskNotFoundError(f"Data for task {task_id} not found in storage.")
            
            # Update status to processing
            await self.redis_client.hset(task_key, "status", "processing")
            task_data["status"] = "processing"
            
            # Deserialize payload
            task_data["payload"] = json.loads(task_data.get("payload", "{}"))
            return task_data
        except redis.RedisError as e:
            raise QueueError(f"Redis error during dequeue: {e}")

    async def peek(self) -> Optional[Dict[str, Any]]:
        """
        Returns the data of the task at the front of the queue without dequeuing it.
        Returns None if the queue is empty.
        """
        try:
            task_id = await self.redis_client.lindex(self.queue_key, 0)
            if not task_id:
                return None

            task_key = f"task:{task_id}"
            task_data = await self.redis_client.hgetall(task_key)
            
            if not task_data:
                raise TaskNotFoundError(f"Data for task {task_id} not found in storage.")
            
            task_data["payload"] = json.loads(task_data.get("payload", "{}"))
            return task_data
        except redis.RedisError as e:
            raise QueueError(f"Redis error during peek: {e}")

    async def queue_length(self) -> int:
        """
        Returns the number of tasks currently in the queue.
        """
        try:
            return await self.redis_client.llen(self.queue_key)
        except redis.RedisError as e:
            raise QueueError(f"Redis error fetching queue length: {e}")

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a task's data by its ID.
        """
        task_key = f"task:{task_id}"
        try:
            task_data = await self.redis_client.hgetall(task_key)
            if not task_data:
                return None
            
            task_data["payload"] = json.loads(task_data.get("payload", "{}"))
            return task_data
        except redis.RedisError as e:
            raise QueueError(f"Redis error fetching task: {e}")

    async def close(self):
        """
        Closes the Redis connection.
        """
        await self.redis_client.close()
