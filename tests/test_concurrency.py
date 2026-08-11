import pytest
import asyncio
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_concurrent_enqueue(queue):
    num_tasks = 100
    
    # Enqueue tasks concurrently
    tasks = [queue.enqueue({"id": i}) for i in range(num_tasks)]
    await asyncio.gather(*tasks)
    
    length = await queue.queue_length()
    assert length == num_tasks
    
    # Check that they can all be dequeued properly
    dequeued_count = 0
    while True:
        task = await queue.dequeue()
        if not task:
            break
        dequeued_count += 1
        
    assert dequeued_count == num_tasks

@pytest.mark.asyncio
async def test_concurrent_api_requests(client: AsyncClient):
    num_requests = 20
    
    # We will spam the API concurrently
    tasks = [client.post("/tasks/", json={"payload": {"req": i}}) for i in range(num_requests)]
    responses = await asyncio.gather(*tasks)
    
    for resp in responses:
        assert resp.status_code == 200
        
    # Check that all tasks are listed
    list_resp = await client.get("/tasks/?limit=100")
    assert list_resp.status_code == 200
    assert len(list_resp.json()["tasks"]) == num_requests
