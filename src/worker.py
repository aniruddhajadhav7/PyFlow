import asyncio
import inspect
import logging
import os
import signal
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Any, Optional
from src.queue import RedisQueue, TaskTimeoutError
from src.config import settings

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, redis_url: str, queues: list[str] = None, max_concurrent_tasks: int = 100, worker_id: str = None):
        self.queues = queues or ["default"]
        self.queue = RedisQueue(redis_url=redis_url)
        self.shutdown_event = asyncio.Event()
        self.active_tasks = set()
        self.task_registry: Dict[str, Callable] = {}
        self.task_timeouts: Dict[str, Optional[float]] = {}
        self.max_concurrent_tasks = max_concurrent_tasks

        # Node identification and heartbeat configuration
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.worker_id = worker_id or f"worker-{self.hostname}-{self.pid}-{uuid.uuid4().hex[:6]}"
        self.started_at = time.time()
        self.heartbeat_interval = 5.0
        self.heartbeat_ttl = 15
        self._heartbeat_task = None

    def task(self, task_name: str, timeout: Optional[float] = None):
        """
        Decorator to register a task handler with an optional default timeout in seconds.
        """
        def decorator(func: Callable):
            self.task_registry[task_name] = func
            if timeout is not None:
                self.task_timeouts[task_name] = timeout
            return func
        return decorator

    async def _process_task(self, task: dict):
        """
        Looks up the task handler and executes it within the effective timeout.
        """
        task_id = task.get("id")
        task_name = task.get("task_name")
        payload = task.get("payload", {})
        queue_name = task.get("queue_name", "default")

        # Determine effective timeout: runtime override > decorator default > global settings
        effective_timeout = task.get("timeout")
        if effective_timeout is None:
            effective_timeout = self.task_timeouts.get(task_name, settings.default_task_timeout)

        logger.info(
            f"Processing task {task_id} (type: {task_name}, timeout: {effective_timeout}s)..."
        )
        try:
            handler = self.task_registry.get(task_name)
            if not handler:
                raise ValueError(f"No handler registered for task type: '{task_name}'")

            coro = handler(payload) if inspect.iscoroutinefunction(handler) else asyncio.to_thread(handler, payload)

            if effective_timeout is not None and effective_timeout > 0:
                try:
                    result = await asyncio.wait_for(coro, timeout=float(effective_timeout))
                except asyncio.TimeoutError:
                    raise TaskTimeoutError(
                        f"Task '{task_name}' (ID: {task_id}) exceeded execution timeout of {effective_timeout}s"
                    )
            else:
                result = await coro

            # On success
            await self.queue.update_task_status(task_id, "SUCCESS", result=result, ttl=settings.task_result_ttl)
            logger.info(f"Task {task_id} completed successfully.")

            # Persist to DB and remove from Redis
            from src.db import AsyncSessionLocal
            from src.models import TaskLog
            import uuid
            async with AsyncSessionLocal() as session:
                log = TaskLog(
                    id=uuid.UUID(task_id),
                    task_name=task_name,
                    queue_name=queue_name,
                    status="SUCCESS",
                    payload=payload,
                    result=result,
                    error=None,
                    completed_at=datetime.now(timezone.utc),
                )
                session.add(log)
                await session.commit()
            await self.queue.delete_task(task_id, queue_name=queue_name)

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            permanently_failed = await self.queue.fail_task(
                task_id,
                str(e),
                max_retries=settings.task_max_retries,
                base_delay=settings.task_retry_base_delay,
                ttl=settings.task_result_ttl,
            )
            if permanently_failed:
                # Persist to DB but keep in Redis DLQ so the /tasks/failed API works
                from src.db import AsyncSessionLocal
                from src.models import TaskLog
                import uuid
                async with AsyncSessionLocal() as session:
                    log = TaskLog(
                        id=uuid.UUID(task_id),
                        task_name=task_name,
                        queue_name=queue_name,
                        status="FAILED",
                        payload=payload,
                        result=None,
                        error=str(e),
                        completed_at=datetime.now(timezone.utc),
                    )
                    session.add(log)
                    await session.commit()

    async def _handle_task(self, task: dict):
        """
        Wrapper to track active tasks and handle processing.
        """
        task_obj = asyncio.current_task()
        self.active_tasks.add(task_obj)
        try:
            await self._process_task(task)
        finally:
            self.active_tasks.discard(task_obj)
            self.semaphore.release()

    async def _send_heartbeat(self, status: str = "ACTIVE"):
        """
        Sends a heartbeat to Redis recording worker metadata and active task load.
        """
        now = time.time()
        worker_key = f"worker:{self.worker_id}"
        mapping = {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "pid": str(self.pid),
            "queues": ",".join(self.queues),
            "concurrency": str(self.max_concurrent_tasks),
            "active_tasks": str(len(self.active_tasks)),
            "started_at": str(self.started_at),
            "last_heartbeat": str(now),
            "status": status,
        }
        try:
            async with self.queue.redis_client.pipeline(transaction=True) as pipe:
                pipe.sadd("workers:active", self.worker_id)
                pipe.hset(worker_key, mapping=mapping)
                pipe.expire(worker_key, self.heartbeat_ttl)
                await pipe.execute()
        except Exception as e:
            logger.warning(f"Failed to send heartbeat for worker {self.worker_id}: {e}")

    async def _heartbeat_loop(self):
        """
        Background loop to send periodic heartbeats while the worker is running.
        """
        try:
            while not self.shutdown_event.is_set():
                await self._send_heartbeat(status="ACTIVE")
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.heartbeat_interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def _deregister(self):
        """
        Removes the worker registration and heartbeat key from Redis on shutdown.
        """
        worker_key = f"worker:{self.worker_id}"
        try:
            async with self.queue.redis_client.pipeline(transaction=True) as pipe:
                pipe.srem("workers:active", self.worker_id)
                pipe.delete(worker_key)
                await pipe.execute()
            logger.info(f"Worker {self.worker_id} successfully deregistered.")
        except Exception as e:
            logger.warning(f"Failed to deregister worker {self.worker_id}: {e}")

    async def _get_queues_to_poll(self) -> list[str]:
        """Resolve which queues to poll. If '*' is specified, fetch known queues."""
        if "*" in self.queues:
            known = await self.queue.get_known_queues()
            return known if known else ["default"]
        return self.queues

    async def run(self):
        """
        Main worker loop.
        """
        logger.info("Worker started. Waiting for tasks...")

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown_event.set)

        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

        # Start periodic background heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            while not self.shutdown_event.is_set():
                queues_to_poll = await self._get_queues_to_poll()
                
                # Poll for scheduled/cron tasks
                await self.queue.poll_scheduled_tasks()

                # Poll for delayed tasks before dequeuing
                await self.queue.poll_delayed_tasks(queues_to_poll)

                # Wait for capacity, allowing shutdown check every second
                try:
                    await asyncio.wait_for(self.semaphore.acquire(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                task = await self.queue.dequeue(queues_to_poll)
                if task:
                    logger.info(f"Dequeued task {task['id']}")
                    # Run task asynchronously without blocking the consumer loop
                    asyncio.create_task(self._handle_task(task))
                else:
                    self.semaphore.release()
                    # Queue is empty, wait a bit before polling again
                    try:
                        await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
        finally:
            logger.info("Worker shutting down...")
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            await self._send_heartbeat(status="SHUTTING_DOWN")

            if self.active_tasks:
                logger.info(
                    f"Waiting for {len(self.active_tasks)} active tasks to finish..."
                )
                await asyncio.gather(*self.active_tasks, return_exceptions=True)

            await self._deregister()
            await self.queue.close()
            logger.info("Worker shutdown complete.")


if __name__ == "__main__":
    from src.logger import setup_logging
    setup_logging()
    
    queues = [q.strip() for q in settings.worker_queues.split(",")]
    worker = Worker(
        redis_url=settings.redis_url,
        queues=queues,
        max_concurrent_tasks=settings.worker_concurrency
    )

    @worker.task("test_task")
    async def handle_test_task(payload):
        logger.info(f"Test task executed with payload: {payload}")
        await asyncio.sleep(1)

    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        pass
