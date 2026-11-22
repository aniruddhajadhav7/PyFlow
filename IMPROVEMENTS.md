# PyFlow Improvements Roadmap

This file tracks technical additions and improvements to gradually develop PyFlow from a prototype into a robust, production-ready framework.

## ~~1. 🏗️ Task Registry and Dynamic Handlers (Critical)~~ (Implemented)
**Current State:** Fully dynamic task registry with decorator-based routing (`@worker.task(name)`).
- Workers dynamically lookup and execute the registered function based on task name.

## ~~2. 🚦 Worker Concurrency Limiting (Critical for Stability)~~ (Implemented)
**Current State:** Bounded worker concurrency using `asyncio.Semaphore(max_concurrent_tasks)`.
- Prevents memory exhaustion or crashing downstream services under high volume.

## ~~3. 📦 Result Storage and Expiration~~ (Implemented)
**Current State:** Tasks store execution results in Redis hashes with automated TTL expiration (`task_result_ttl`).

## ~~4. 🔀 Multi-Queue Support (Task Routing)~~ (Implemented)
**Current State:** Dynamic multi-queue routing (`queue:{name}`), known queue discovery, and worker support for specific named queues or `*` wildcard.

## ~~5. 🕒 Periodic / Cron Tasks~~ (Implemented)
**Current State:** Native cron scheduler using Redis Sorted Sets (`schedules:execution`), standard cron expressions via `croniter`, and atomic worker polling.

## ~~6. 📊 Dead Letter Queue (DLQ) Management via API~~ (Implemented)
**Current State:** Dedicated DLQ endpoints (`GET /tasks/failed` with pagination, `DELETE /tasks/failed` for atomic bulk clearing, and `POST /tasks/{id}/retry`).

## ~~7. 🐘 PostgreSQL / Relational DB Integration~~ (Implemented)
**Current State:** Persistent task audit log storage via SQLAlchemy (`TaskLog`) in PostgreSQL with `GET /tasks/history` API.

## ~~8. 🖥️ Monitoring Dashboard~~ (Implemented)
**Current State:** Real-time web monitoring dashboard served directly by FastAPI at `/dashboard`, supported by the aggregated metrics API at `/dashboard/stats`.
- Live visual overview of queue depths, pending/delayed/DLQ counters, cron schedules, and audit history with zero external build tools required.

## ~~9. ⏱️ Task Timeouts~~ (Implemented)
**Current State:** Fully configurable task execution timeouts using non-blocking `asyncio.wait_for`.
- Configurable per-handler via `@worker.task("name", timeout=30)`, per-submission via `POST /tasks/ {"timeout": 30}`, or globally in `Settings.default_task_timeout`.
- Automatically raises `TaskTimeoutError`, triggers backoff retries, and prevents runaway tasks from deadlocking worker concurrency slots.

## ~~10. 💓 Worker Heartbeat, Auto-Registration & Node Discovery~~ (Implemented)
**Current State:** Fully distributed worker node heartbeat, dynamic cluster registry, and live health monitoring.
- Background worker heartbeat loop sending periodic telemetry to Redis (`workers:active` and `worker:{id}`).
- Tracks worker ID, hostname, PID, assigned queues, concurrency limits, active task counts, and node uptime.
- Exposes `GET /workers` and `GET /workers/{worker_id}` management API.
- Embedded "Worker Nodes" live monitoring grid in `/dashboard` with auto-pruning of dead/stale nodes.
