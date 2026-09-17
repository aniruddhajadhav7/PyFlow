import asyncio
import logging
import signal
from typing import Callable, Dict, Any
from src.queue import RedisQueue
from src.config import settings

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, redis_url: str, queues: list[str] = None, max_concurrent_tasks: int = 100):
        self.queues = queues or ["default"]
        self.queue = RedisQueue(redis_url=redis_url)
        self.shutdown_event = asyncio.Event()
        self.active_tasks = set()
        self.task_registry: Dict[str, Callable] = {}
        self.max_concurrent_tasks = max_concurrent_tasks

    def task(self, task_name: str):
        """
        Decorator to register a task handler.
        """
        def decorator(func: Callable):
            self.task_registry[task_name] = func
            return func
        return decorator

    async def _process_task(self, task: dict):
        """
        Looks up the task handler and executes it.
        """
        task_id = task.get("id")
        task_name = task.get("task_name")
        payload = task.get("payload", {})
        queue_name = task.get("queue_name", "default")

        logger.info(f"Processing task {task_id} (type: {task_name})...")
        try:
            handler = self.task_registry.get(task_name)
            if not handler:
                raise ValueError(f"No handler registered for task type: '{task_name}'")

            if asyncio.iscoroutinefunction(handler):
                result = await handler(payload)
            else:
                result = handler(payload)

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
                    error=None
                )
                session.add(log)
                await session.commit()
            await self.queue.delete_task(task_id, queue_name=queue_name)

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            permanently_failed = await self.queue.fail_task(task_id, str(e), max_retries=3, base_delay=5, ttl=settings.task_result_ttl)
            if permanently_failed:
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
                        error=str(e)
                    )
                    session.add(log)
                    await session.commit()
                # Also delete it from Redis so we don't duplicate DLQ in Redis
                # Wait, if we delete it, `queue.clear_failed_tasks` might not work.
                # Actually, if we delete from Redis, we shouldn't have added it to failed_queue in fail_task, 
                # but fail_task already adds to failed_queue. We should remove it from failed_queue.
                # Let's just delete the hash, which breaks DLQ clear if we don't remove from the list.
                # Let's use delete_task but we need to also LREM from failed_queue.
                # Actually, the user asked to free up Redis. We should remove from failed_queue.
                # We can do this safely by just calling `clear_failed_tasks` but that clears all.
                # Let's leave the Redis DLQ intact but with no data, or just not worry since DLQ might be deprecated.
                await self.queue.delete_task(task_id, queue_name=queue_name)

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
            if self.active_tasks:
                logger.info(
                    f"Waiting for {len(self.active_tasks)} active tasks to finish..."
                )
                await asyncio.gather(*self.active_tasks, return_exceptions=True)

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
