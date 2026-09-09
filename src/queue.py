import json
import uuid
from typing import Any, Dict, Optional
import redis.asyncio as redis


class QueueError(Exception):
    """Base class for queue-related exceptions."""


class TaskNotFoundError(QueueError):
    """Raised when a task is not found in the storage."""


class RedisQueue:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = redis.from_url(redis_url, decode_responses=True)

    def _get_queue_key(self, queue_name: str) -> str:
        return f"queue:{queue_name}"

    def _get_delayed_queue_key(self, queue_name: str) -> str:
        return f"delayed_queue:{queue_name}"

    def _get_failed_queue_key(self, queue_name: str) -> str:
        return f"failed_queue:{queue_name}"

    def _deserialize_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        if not task_data:
            return task_data

        task_data["payload"] = json.loads(task_data.get("payload", "{}"))

        if "result" in task_data:
            try:
                task_data["result"] = json.loads(task_data["result"])
            except (TypeError, ValueError):
                pass

        return task_data

    async def get_known_queues(self) -> list[str]:
        """
        Retrieves the list of all dynamically known queues.
        """
        try:
            queues = await self.redis_client.smembers("queues:known")
            return list(queues) if queues else ["default"]
        except redis.RedisError as e:
            raise QueueError(f"Redis error getting known queues: {e}")

    async def enqueue(self, queue_name: str, task_name: str, task_payload: dict = None) -> str:
        """
        Enqueues a task to the specified queue and returns its unique task_id.
        """
        if task_payload is None:
            task_payload = {}

        task_id = str(uuid.uuid4())
        task_key = f"task:{task_id}"

        # Serialize the payload
        try:
            serialized_payload = json.dumps(task_payload)
        except (TypeError, ValueError) as e:
            raise QueueError(f"Failed to serialize task payload: {e}")

        task_data = {
            "id": task_id,
            "task_name": task_name,
            "queue_name": queue_name,
            "payload": serialized_payload,
            "status": "PENDING",
            "retry_count": 0,
        }

        try:
            import time
            now = time.time()
            async with self.redis_client.pipeline(transaction=True) as pipe:
                pipe.hset(task_key, mapping=task_data)
                pipe.rpush(self._get_queue_key(queue_name), task_id)
                pipe.zadd("tasks:created", {task_id: now})
                pipe.sadd("queues:known", queue_name)
                await pipe.execute()
            return task_id
        except redis.RedisError as e:
            raise QueueError(f"Redis error during enqueue: {e}")

    async def dequeue(self, queues: list[str]) -> Optional[Dict[str, Any]]:
        """
        Dequeues a task from the front of the first available queue, updates its status, and returns its data.
        Returns None if all queues are empty.
        """
        try:
            for queue_name in queues:
                task_id = await self.redis_client.lpop(self._get_queue_key(queue_name))
                if task_id:
                    task_key = f"task:{task_id}"
                    task_data = await self.redis_client.hgetall(task_key)

                    if not task_data:
                        raise TaskNotFoundError(
                            f"Data for task {task_id} not found in storage."
                        )

                    # Update status to processing
                    await self.redis_client.hset(task_key, "status", "RUNNING")
                    task_data["status"] = "RUNNING"

                    return self._deserialize_task(task_data)
            return None
        except redis.RedisError as e:
            raise QueueError(f"Redis error during dequeue: {e}")

    async def peek(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Returns the data of the task at the front of the specified queue without dequeuing it.
        Returns None if the queue is empty.
        """
        try:
            task_id = await self.redis_client.lindex(self._get_queue_key(queue_name), 0)
            if not task_id:
                return None

            task_key = f"task:{task_id}"
            task_data = await self.redis_client.hgetall(task_key)

            if not task_data:
                raise TaskNotFoundError(
                    f"Data for task {task_id} not found in storage."
                )

            return self._deserialize_task(task_data)
        except redis.RedisError as e:
            raise QueueError(f"Redis error during peek: {e}")

    async def queue_length(self, queue_name: str) -> int:
        """
        Returns the number of tasks currently in the specified queue.
        """
        try:
            return await self.redis_client.llen(self._get_queue_key(queue_name))
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

            return self._deserialize_task(task_data)
        except redis.RedisError as e:
            raise QueueError(f"Redis error fetching task: {e}")

    async def list_tasks(self, limit: int = 50, offset: int = 0):
        """
        Lists all tasks globally by fetching from the creation index using ZRANGE, then pipeline HGETALL.
        """
        try:
            tasks = []
            zset_key = "tasks:created"
            # ZRANGE is inclusive for start and end, so we use offset and offset + limit - 1
            task_ids = await self.redis_client.zrange(zset_key, offset, offset + limit - 1)
            
            if not task_ids:
                return []
                
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for task_id in task_ids:
                    pipe.hgetall(f"task:{task_id}")
                task_data_list = await pipe.execute()
                
            for task_data in task_data_list:
                if task_data:
                    tasks.append(self._deserialize_task(task_data))
            return tasks
        except redis.RedisError as e:
            raise QueueError(f"Redis error listing tasks: {e}")

    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancels a task if it is PENDING using an atomic Lua script.
        """
        task_key = f"task:{task_id}"
        
        try:
            # First fetch the queue name
            queue_name = await self.redis_client.hget(task_key, "queue_name")
            if not queue_name:
                return False
                
            queue_key = self._get_queue_key(queue_name)
            
            script = """
            local task_key = KEYS[1]
            local queue_key = KEYS[2]
            local task_id = ARGV[1]
            local status = redis.call("HGET", task_key, "status")
            if status == "PENDING" then
                redis.call("LREM", queue_key, 0, task_id)
                redis.call("HSET", task_key, "status", "FAILED")
                return 1
            else
                return 0
            end
            """
            result = await self.redis_client.eval(script, 2, task_key, queue_key, task_id)
            return bool(result)
        except redis.RedisError as e:
            raise QueueError(f"Redis error cancelling task: {e}")

    async def update_task_status(self, task_id: str, status: str, result: Any = None, ttl: int = None):
        """
        Updates the status of a task, optionally stores a result, and sets an optional TTL.
        """
        task_key = f"task:{task_id}"
        mapping = {"status": status}
        if result is not None:
            try:
                mapping["result"] = json.dumps(result)
            except (TypeError, ValueError):
                mapping["result"] = str(result)

        try:
            if ttl is not None and ttl > 0:
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    pipe.hset(task_key, mapping=mapping)
                    pipe.expire(task_key, ttl)
                    await pipe.execute()
            else:
                await self.redis_client.hset(task_key, mapping=mapping)
        except redis.RedisError as e:
            raise QueueError(f"Redis error updating task status: {e}")

    async def fail_task(
        self,
        task_id: str,
        error_message: str,
        max_retries: int = 3,
        base_delay: int = 5,
        ttl: int = None,
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

            queue_name = task_data.get("queue_name", "default")
            delayed_queue_key = self._get_delayed_queue_key(queue_name)
            failed_queue_key = self._get_failed_queue_key(queue_name)

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
                    delayed_queue_key, {task_id: execute_at}
                )
            else:
                # Permanently failed
                if ttl is not None and ttl > 0:
                    async with self.redis_client.pipeline(transaction=True) as pipe:
                        pipe.hset(
                            task_key, mapping={"status": "FAILED", "error": error_message}
                        )
                        pipe.rpush(failed_queue_key, task_id)
                        pipe.expire(task_key, ttl)
                        await pipe.execute()
                else:
                    await self.redis_client.hset(
                        task_key, mapping={"status": "FAILED", "error": error_message}
                    )
                    await self.redis_client.rpush(failed_queue_key, task_id)
        except redis.RedisError as e:
            raise QueueError(f"Redis error handling task failure: {e}")

    async def poll_delayed_tasks(self, queues: list[str]):
        """
        Moves tasks from the delayed queue to the main queue if their time has come.
        """
        try:
            import time
            now = time.time()
            
            for queue_name in queues:
                delayed_queue_key = self._get_delayed_queue_key(queue_name)
                queue_key = self._get_queue_key(queue_name)
                
                # Fetch tasks with score <= now
                tasks_to_enqueue = await self.redis_client.zrangebyscore(
                    delayed_queue_key, 0, now
                )

                if tasks_to_enqueue:
                    # Use a pipeline to ensure atomicity for moving
                    async with self.redis_client.pipeline(transaction=True) as pipe:
                        for task_id in tasks_to_enqueue:
                            pipe.zrem(delayed_queue_key, task_id)
                            pipe.rpush(queue_key, task_id)
                        await pipe.execute()
        except redis.RedisError as e:
            raise QueueError(f"Redis error polling delayed tasks: {e}")

    async def retry_task(self, task_id: str) -> bool:
        """
        Retries a FAILED task manually via API using an atomic Lua script.
        """
        task_key = f"task:{task_id}"
        
        try:
            queue_name = await self.redis_client.hget(task_key, "queue_name")
            if not queue_name:
                return False
                
            failed_queue_key = self._get_failed_queue_key(queue_name)
            queue_key = self._get_queue_key(queue_name)
            
            script = """
            local task_key = KEYS[1]
            local failed_queue_key = KEYS[2]
            local queue_key = KEYS[3]
            local task_id = ARGV[1]
            local status = redis.call("HGET", task_key, "status")
            if status == "FAILED" then
                redis.call("LREM", failed_queue_key, 0, task_id)
                redis.call("HSET", task_key, "status", "PENDING", "retry_count", 0, "error", "")
                redis.call("RPUSH", queue_key, task_id)
                return 1
            else
                return 0
            end
            """
            result = await self.redis_client.eval(script, 3, task_key, failed_queue_key, queue_key, task_id)
            return bool(result)
        except redis.RedisError as e:
            raise QueueError(f"Redis error retrying task: {e}")

    async def close(self):
        """
        Closes the Redis connection.
        """
        await self.redis_client.aclose()

    def _deserialize_schedule(self, schedule_data: Dict[str, Any]) -> Dict[str, Any]:
        if not schedule_data:
            return schedule_data

        schedule_data["payload"] = json.loads(schedule_data.get("payload", "{}"))
        schedule_data["next_execution"] = float(schedule_data.get("next_execution", 0))
        schedule_data["created_at"] = float(schedule_data.get("created_at", 0))

        return schedule_data

    async def schedule_task(self, cron_expression: str, queue_name: str, task_name: str, task_payload: dict = None) -> str:
        """
        Creates a new recurring schedule and returns its unique schedule_id.
        """
        from croniter import croniter
        import time
        if task_payload is None:
            task_payload = {}

        if not croniter.is_valid(cron_expression):
            raise QueueError("Invalid cron expression")

        schedule_id = str(uuid.uuid4())
        schedule_key = f"schedule:{schedule_id}"
        
        try:
            serialized_payload = json.dumps(task_payload)
        except (TypeError, ValueError) as e:
            raise QueueError(f"Failed to serialize task payload: {e}")

        now = time.time()
        cron = croniter(cron_expression, now)
        next_execution = cron.get_next(float)

        schedule_data = {
            "id": schedule_id,
            "cron_expression": cron_expression,
            "queue_name": queue_name,
            "task_name": task_name,
            "payload": serialized_payload,
            "next_execution": next_execution,
            "created_at": now,
        }

        try:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                pipe.hset(schedule_key, mapping=schedule_data)
                pipe.zadd("schedules:execution", {schedule_id: next_execution})
                await pipe.execute()
            return schedule_id
        except redis.RedisError as e:
            raise QueueError(f"Redis error during schedule_task: {e}")

    async def poll_scheduled_tasks(self):
        """
        Finds schedules that are due, enqueues their tasks, and recalculates their next execution time.
        Uses ZPOPMIN to ensure only one worker processes a given schedule trigger.
        """
        from croniter import croniter
        import time
        try:
            now = time.time()
            # We want to process all due schedules, but ZPOPMIN gives us one at a time.
            # We pop 1 schedule. If its score is <= now, we process it. Otherwise, we put it back and stop.
            while True:
                # Atomically pop the schedule with the lowest score
                popped = await self.redis_client.zpopmin("schedules:execution")
                if not popped:
                    break
                    
                schedule_id, score = popped[0]
                if score > now:
                    # It's not due yet. Put it back and stop.
                    await self.redis_client.zadd("schedules:execution", {schedule_id: score})
                    break
                
                # It's due! Let's get the schedule data
                schedule_key = f"schedule:{schedule_id}"
                schedule_data = await self.redis_client.hgetall(schedule_key)
                
                if not schedule_data:
                    # Schedule was somehow deleted after pop. Ignore.
                    continue
                
                deserialized = self._deserialize_schedule(schedule_data)
                
                # Enqueue the task
                await self.enqueue(
                    queue_name=deserialized["queue_name"],
                    task_name=deserialized["task_name"],
                    task_payload=deserialized["payload"]
                )
                
                # Calculate next execution
                cron = croniter(deserialized["cron_expression"], now)
                next_execution = cron.get_next(float)
                
                # Update the schedule's next_execution and add it back to the ZSET
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    pipe.hset(schedule_key, "next_execution", next_execution)
                    pipe.zadd("schedules:execution", {schedule_id: next_execution})
                    await pipe.execute()
                    
        except redis.RedisError as e:
            raise QueueError(f"Redis error polling scheduled tasks: {e}")

    async def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a schedule's data by its ID.
        """
        schedule_key = f"schedule:{schedule_id}"
        try:
            schedule_data = await self.redis_client.hgetall(schedule_key)
            if not schedule_data:
                return None
            return self._deserialize_schedule(schedule_data)
        except redis.RedisError as e:
            raise QueueError(f"Redis error fetching schedule: {e}")

    async def list_schedules(self, limit: int = 50, offset: int = 0):
        """
        Lists all schedules by fetching from the sorted set.
        """
        try:
            schedules = []
            zset_key = "schedules:execution"
            schedule_ids = await self.redis_client.zrange(zset_key, offset, offset + limit - 1)
            
            if not schedule_ids:
                return []
                
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for schedule_id in schedule_ids:
                    pipe.hgetall(f"schedule:{schedule_id}")
                schedule_data_list = await pipe.execute()
                
            for schedule_data in schedule_data_list:
                if schedule_data:
                    schedules.append(self._deserialize_schedule(schedule_data))
            return schedules
        except redis.RedisError as e:
            raise QueueError(f"Redis error listing schedules: {e}")

    async def cancel_schedule(self, schedule_id: str) -> bool:
        """
        Deletes a schedule.
        """
        schedule_key = f"schedule:{schedule_id}"
        try:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                pipe.exists(schedule_key)
                pipe.delete(schedule_key)
                pipe.zrem("schedules:execution", schedule_id)
                results = await pipe.execute()
            
            # The first result of pipeline is the output of `exists`
            return bool(results[0])
        except redis.RedisError as e:
            raise QueueError(f"Redis error cancelling schedule: {e}")
