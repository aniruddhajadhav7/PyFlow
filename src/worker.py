import asyncio
import logging
import signal
from typing import Optional
from src.queue import RedisQueue, TaskNotFoundError
from src.config import settings

logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, redis_url: str, queue_name: str = "default_queue"):
        self.queue = RedisQueue(redis_url=redis_url, queue_name=queue_name)
        self.shutdown_event = asyncio.Event()
        self.active_tasks = set()

    async def _process_task(self, task: dict):
        """
        Simulated task processing logic.
        """
        task_id = task.get("id")
        payload = task.get("payload", {})
        
        logger.info(f"Processing task {task_id}...")
        try:
            # Simulate processing delay
            await asyncio.sleep(2)
            
            # Simple simulation: if payload has a 'fail' key, we fail the task
            if payload.get("fail"):
                raise ValueError("Simulated task failure.")
            
            # On success
            await self.queue.update_task_status(task_id, "SUCCESS")
            logger.info(f"Task {task_id} completed successfully.")
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            await self.queue.update_task_status(task_id, "FAILED")

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

    async def run(self):
        """
        Main worker loop.
        """
        logger.info("Worker started. Waiting for tasks...")
        
        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown_event.set)

        try:
            while not self.shutdown_event.is_set():
                # We use a short sleep to prevent tight looping if dequeue is fast and empty
                task = await self.queue.dequeue()
                if task:
                    logger.info(f"Dequeued task {task['id']}")
                    # Run task asynchronously without blocking the consumer loop
                    asyncio.create_task(self._handle_task(task))
                else:
                    # Queue is empty, wait a bit before polling again
                    try:
                        await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
        finally:
            logger.info("Worker shutting down...")
            if self.active_tasks:
                logger.info(f"Waiting for {len(self.active_tasks)} active tasks to finish...")
                await asyncio.gather(*self.active_tasks, return_exceptions=True)
            
            await self.queue.close()
            logger.info("Worker shutdown complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = Worker(redis_url=settings.redis_url)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        pass
