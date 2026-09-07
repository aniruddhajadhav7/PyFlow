import asyncio
import logging
import signal
from typing import Callable, Dict, Any
from src.queue import RedisQueue
from src.config import settings

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, redis_url: str, queue_name: str = "default_queue", max_concurrent_tasks: int = 100):
        self.queue = RedisQueue(redis_url=redis_url, queue_name=queue_name)
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
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            await self.queue.fail_task(task_id, str(e), max_retries=3, base_delay=5, ttl=settings.task_result_ttl)

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
                # Poll for delayed tasks before dequeuing
                await self.queue.poll_delayed_tasks()

                # Wait for capacity, allowing shutdown check every second
                try:
                    await asyncio.wait_for(self.semaphore.acquire(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                task = await self.queue.dequeue()
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
    worker = Worker(
        redis_url=settings.redis_url,
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
