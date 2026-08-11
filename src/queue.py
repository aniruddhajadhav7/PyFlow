import json
import uuid
from typing import Any, Dict, Optional
import redis.asyncio as redis


class QueueError(Exception):
    """Base class for queue-related exceptions."""


class TaskNotFoundError(QueueError):
    """Raised when a task is not found in the storage."""


class RedisQueue:
    def __init__(self, redis_url: str, queue_name: str = "default_queue"):
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.queue_key = f"queue:{self.queue_name}"
        self.delayed_queue_key = f"delayed_queue:{self.queue_name}"
        self.failed_queue_key = f"failed_queue:{self.queue_name}"

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
            "status": "PENDING",
            "retry_count": 0,
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
                raise TaskNotFoundError(
                    f"Data for task {task_id} not found in storage."
                )

            # Update status to processing
            await self.redis_client.hset(task_key, "status", "RUNNING")
            task_data["status"] = "RUNNING"

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
                raise TaskNotFoundError(
                    f"Data for task {task_id} not found in storage."
                )

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

    async def list_tasks(self, limit: int = 50, offset: int = 0):
        """
        Lists tasks by scanning for task keys.
        """
        try:
            tasks = []
            cursor, keys = await self.redis_client.scan(
                cursor=0, match="task:*", count=1000
            )
            # Simplistic pagination (in a real app, use better pagination)
            keys = keys[offset : offset + limit]
            for key in keys:
                task_data = await self.redis_client.hgetall(key)
                if task_data:
                    task_data["payload"] = json.loads(task_data.get("payload", "{}"))
                    tasks.append(task_data)
            return tasks
        except redis.RedisError as e:
            raise QueueError(f"Redis error listing tasks: {e}")

    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancels a task if it is PENDING.
        """
        task_key = f"task:{task_id}"
        try:
            status = await self.redis_client.hget(task_key, "status")
            if status != "PENDING":
                return False

            # Remove from queue list
            await self.redis_client.lrem(self.queue_key, 0, task_id)
            await self.redis_client.hset(task_key, "status", "FAILED")
            return True
        except redis.RedisError as e:
            raise QueueError(f"Redis error cancelling task: {e}")

    async def update_task_status(self, task_id: str, status: str):
        """
        Updates the status of a task.
        """
        task_key = f"task:{task_id}"
        try:
            await self.redis_client.hset(task_key, "status", status)
        except redis.RedisError as e:
            raise QueueError(f"Redis error updating task status: {e}")

    async def fail_task(
        self,
        task_id: str,
        error_message: str,
        max_retries: int = 3,
        base_delay: int = 5,
    ):
        """
        Handles a task failure. If retries remain, calculates exponential backoff and puts in delayed queue.
        Otherwise, moves it to the permanently failed queue.
        """
        task_key = f"task:{task_id}"
        try:
            task_data = await self.redis_client.hgetall(task_key)
            if not task_data:
                return

            retry_count = int(task_data.get("retry_count", 0))
            if retry_count < max_retries:
                # Exponential backoff: base_delay * (2 ^ retry_count)
                delay = base_delay * (2**retry_count)
                import time

                execute_at = time.time() + delay

                await self.redis_client.hset(
                    task_key,
                    mapping={
                        "status": "PENDING",
                        "retry_count": retry_count + 1,
                        "error": error_message,
                    },
                )
                # Add to delayed sorted set
                await self.redis_client.zadd(
                    self.delayed_queue_key, {task_id: execute_at}
                )
            else:
                # Permanently failed
                await self.redis_client.hset(
                    task_key, mapping={"status": "FAILED", "error": error_message}
                )
                await self.redis_client.rpush(self.failed_queue_key, task_id)
        except redis.RedisError as e:
            raise QueueError(f"Redis error handling task failure: {e}")

    async def poll_delayed_tasks(self):
        """
        Moves tasks from the delayed queue to the main queue if their time has come.
        """
        try:
            import time

            now = time.time()
            # Fetch tasks with score <= now
            tasks_to_enqueue = await self.redis_client.zrangebyscore(
                self.delayed_queue_key, 0, now
            )

            if tasks_to_enqueue:
                # Use a pipeline to ensure atomicity for moving
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    for task_id in tasks_to_enqueue:
                        pipe.zrem(self.delayed_queue_key, task_id)
                        pipe.rpush(self.queue_key, task_id)
                    await pipe.execute()
        except redis.RedisError as e:
            raise QueueError(f"Redis error polling delayed tasks: {e}")

    async def retry_task(self, task_id: str) -> bool:
        """
        Retries a FAILED task manually via API.
        """
        task_key = f"task:{task_id}"
        try:
            status = await self.redis_client.hget(task_key, "status")
            if status != "FAILED":
                return False

            # Remove from failed queue list
            await self.redis_client.lrem(self.failed_queue_key, 0, task_id)

            await self.redis_client.hset(
                task_key, mapping={"status": "PENDING", "retry_count": 0, "error": ""}
            )
            await self.redis_client.rpush(self.queue_key, task_id)
            return True
        except redis.RedisError as e:
            raise QueueError(f"Redis error retrying task: {e}")

    async def close(self):
        """
        Closes the Redis connection.
        """
        await self.redis_client.aclose()
