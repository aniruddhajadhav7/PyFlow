from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from src.queue import RedisQueue
from src.db import get_db, AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc
from src.models import TaskLog

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


@router.get("/stats")
async def get_dashboard_stats(
    queue: RedisQueue = Depends(get_queue),
    db: AsyncSession = Depends(get_db)
):
    """
    Returns aggregated metrics for queues, schedules, DLQ, and recent execution history.
    """
    stats = await queue.get_stats()
    
    # Get active schedules
    schedules = await queue.list_schedules(limit=20, offset=0)
    
    # Get recent history from database
    history = []
    try:
        stmt = select(TaskLog).order_by(desc(TaskLog.created_at)).limit(10)
        res = await db.execute(stmt)
        logs = res.scalars().all()
        history = [
            {
                "id": str(log.id),
                "task_name": log.task_name,
                "queue_name": log.queue_name,
                "status": log.status,
                "error": log.error,
                "created_at": log.created_at.isoformat() if log.created_at else None,
                "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            }
            for log in logs
        ]
    except Exception:
        pass

    return {
        **stats,
        "schedules": schedules,
        "recent_history": history,
    }


@router.get("/", response_class=HTMLResponse)
@router.get("", response_class=HTMLResponse)
async def dashboard_view():
    """
    Renders an interactive real-time web monitoring dashboard.
    """
    html_content = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PyFlow Monitoring Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            brand: { 500: '#6366f1', 600: '#4f46e5', 700: '#4338ca' }
          }
        }
      }
    }
  </script>
  <style>
    @keyframes pulse-slow { 0%, 100% { opacity: 1; } 50% { opacity: .4; } }
    .animate-pulse-slow { animation: pulse-slow 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen antialiased flex flex-col font-sans">
  <!-- Top Navigation -->
  <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-40 px-6 py-4 flex items-center justify-between">
    <div class="flex items-center space-x-3">
      <div class="h-9 w-9 rounded-xl bg-gradient-to-tr from-indigo-500 to-purple-500 flex items-center justify-center shadow-lg shadow-indigo-500/20 font-bold text-lg">⚡</div>
      <div>
        <h1 class="text-xl font-bold tracking-tight">PyFlow <span class="text-indigo-400 font-medium text-sm ml-1 px-2 py-0.5 rounded-full bg-indigo-950/60 border border-indigo-800/50">Dashboard</span></h1>
      </div>
    </div>
    <div class="flex items-center space-x-4 text-sm">
      <div class="flex items-center space-x-2 text-xs bg-slate-800/80 border border-slate-700/60 px-3 py-1.5 rounded-lg">
        <span class="relative flex h-2.5 w-2.5">
          <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
          <span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
        </span>
        <span id="sync-status" class="text-slate-300 font-medium">Live (3s)</span>
      </div>
      <button onclick="fetchStats()" class="px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 transition font-medium text-xs flex items-center space-x-1.5 shadow-sm">
        <span>Refresh Now</span>
      </button>
    </div>
  </header>

  <!-- Main Content -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
    <!-- Stat Cards Row -->
    <div class="grid grid-cols-2 md:grid-cols-6 gap-4">
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Active Workers</p>
        <p id="stat-workers" class="text-2xl font-bold text-cyan-400 mt-1">-</p>
        <div id="stat-workers-detail" class="text-[11px] text-slate-500 mt-1">Cluster Nodes</div>
      </div>
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Pending Tasks</p>
        <p id="stat-pending" class="text-2xl font-bold text-white mt-1">-</p>
        <div class="text-[11px] text-slate-500 mt-1">Queued for execution</div>
      </div>
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Delayed (Retrying)</p>
        <p id="stat-delayed" class="text-2xl font-bold text-amber-400 mt-1">-</p>
        <div class="text-[11px] text-slate-500 mt-1">Backoff delay active</div>
      </div>
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Failed (DLQ)</p>
        <p id="stat-failed" class="text-2xl font-bold text-rose-400 mt-1">-</p>
        <div class="text-[11px] text-slate-500 mt-1">Dead-letter queue</div>
      </div>
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Active Schedules</p>
        <p id="stat-schedules" class="text-2xl font-bold text-indigo-400 mt-1">-</p>
        <div class="text-[11px] text-slate-500 mt-1">Cron-based recurring</div>
      </div>
      <div class="bg-slate-900/70 border border-slate-800 p-4 rounded-xl col-span-2 md:col-span-1">
        <p class="text-xs uppercase tracking-wider text-slate-400 font-medium">Redis Memory</p>
        <p id="stat-redis-mem" class="text-2xl font-bold text-emerald-400 mt-1">-</p>
        <div id="stat-redis-ver" class="text-[11px] text-slate-500 mt-1">Redis Broker</div>
      </div>
    </div>

    <!-- Active Worker Nodes Section -->
    <div class="bg-slate-900/70 border border-slate-800 rounded-xl overflow-hidden">
      <div class="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
        <div class="flex items-center space-x-2">
          <h2 class="font-semibold text-slate-200">Worker Nodes</h2>
          <span id="workers-count-badge" class="text-xs px-2 py-0.5 rounded-full bg-slate-800 text-slate-300">0 active</span>
        </div>
        <span class="text-xs text-slate-400">Heartbeat monitoring & load status</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-sm">
          <thead class="bg-slate-950/60 text-slate-400 text-xs uppercase font-medium border-b border-slate-800/80">
            <tr>
              <th class="px-5 py-3">Worker Node</th>
              <th class="px-5 py-3">Host / PID</th>
              <th class="px-5 py-3">Queues</th>
              <th class="px-5 py-3 text-center">Load (Active / Capacity)</th>
              <th class="px-5 py-3 text-right">Uptime</th>
            </tr>
          </thead>
          <tbody id="workers-table-body" class="divide-y divide-slate-800/60">
            <tr><td colspan="5" class="px-5 py-6 text-center text-slate-500">Scanning for active workers...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Active Queues Breakdown -->
    <div class="bg-slate-900/70 border border-slate-800 rounded-xl overflow-hidden">
      <div class="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
        <div class="flex items-center space-x-2">
          <h2 class="font-semibold text-slate-200">Task Queues</h2>
          <span id="queues-count-badge" class="text-xs px-2 py-0.5 rounded-full bg-slate-800 text-slate-300">0 queues</span>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-sm">
          <thead class="bg-slate-950/60 text-slate-400 text-xs uppercase font-medium border-b border-slate-800/80">
            <tr>
              <th class="px-5 py-3">Queue Name</th>
              <th class="px-5 py-3 text-center">Pending</th>
              <th class="px-5 py-3 text-center">Delayed</th>
              <th class="px-5 py-3 text-center">Dead Letter (Failed)</th>
              <th class="px-5 py-3 text-right">Actions</th>
            </tr>
          </thead>
          <tbody id="queues-table-body" class="divide-y divide-slate-800/60">
            <tr><td colspan="5" class="px-5 py-6 text-center text-slate-500">Loading queues...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Two-Column Layout: Cron Schedules & Recent History -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Cron Schedules -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl overflow-hidden flex flex-col">
        <div class="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
          <h2 class="font-semibold text-slate-200">Scheduled / Cron Tasks</h2>
          <span class="text-xs text-slate-400">Recurring Jobs</span>
        </div>
        <div class="flex-1 overflow-x-auto">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-950/60 text-slate-400 text-xs uppercase font-medium border-b border-slate-800/80">
              <tr>
                <th class="px-5 py-2.5">Task Name</th>
                <th class="px-5 py-2.5">Cron Expression</th>
                <th class="px-5 py-2.5">Queue</th>
                <th class="px-5 py-2.5 text-right">Next Run</th>
              </tr>
            </thead>
            <tbody id="schedules-table-body" class="divide-y divide-slate-800/60">
              <tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">No active schedules</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Recent Execution History -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl overflow-hidden flex flex-col">
        <div class="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
          <h2 class="font-semibold text-slate-200">Recent Completed / Failed Tasks</h2>
          <span class="text-xs text-slate-400">PostgreSQL Audit Logs</span>
        </div>
        <div class="flex-1 overflow-x-auto">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-950/60 text-slate-400 text-xs uppercase font-medium border-b border-slate-800/80">
              <tr>
                <th class="px-5 py-2.5">Task</th>
                <th class="px-5 py-2.5">Status</th>
                <th class="px-5 py-2.5">Queue</th>
                <th class="px-5 py-2.5 text-right">Completed At</th>
              </tr>
            </thead>
            <tbody id="history-table-body" class="divide-y divide-slate-800/60">
              <tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">No task history recorded yet</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </main>

  <footer class="border-t border-slate-800/80 py-4 text-center text-xs text-slate-500">
    PyFlow High-Performance Asynchronous Task Queue Framework
  </footer>

  <script>
    async function clearDLQ(queueName) {
      if (!confirm(`Purge all failed tasks in queue '${queueName}'?`)) return;
      try {
        const res = await fetch(`/tasks/failed?queue_name=${encodeURIComponent(queueName)}`, { method: 'DELETE' });
        if (res.ok) {
          fetchStats();
        } else {
          alert('Failed to clear DLQ');
        }
      } catch (err) {
        alert('Network error: ' + err);
      }
    }

    async function fetchStats() {
      try {
        const res = await fetch('/dashboard/stats');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();

        // Cards
        const workers = data.workers || [];
        document.getElementById('stat-workers').innerText = `${workers.length}`;
        const totalCapacity = data.totals.total_worker_capacity || 0;
        const totalActive = data.totals.total_active_worker_tasks || 0;
        document.getElementById('stat-workers-detail').innerText = `${totalActive} / ${totalCapacity} active slots`;

        document.getElementById('stat-pending').innerText = data.totals.pending.toLocaleString();
        document.getElementById('stat-delayed').innerText = data.totals.delayed.toLocaleString();
        document.getElementById('stat-failed').innerText = data.totals.failed.toLocaleString();
        document.getElementById('stat-schedules').innerText = data.totals.schedules.toLocaleString();
        document.getElementById('stat-redis-mem').innerText = data.redis.used_memory_human || 'N/A';
        document.getElementById('stat-redis-ver').innerText = `Redis v${data.redis.version || '?'}`;

        // Workers Table
        document.getElementById('workers-count-badge').innerText = `${workers.length} nodes`;
        const workersBody = document.getElementById('workers-table-body');
        if (workers.length === 0) {
          workersBody.innerHTML = `<tr><td colspan="5" class="px-5 py-6 text-center text-slate-500">
            <span class="inline-flex items-center space-x-1.5 text-amber-400 text-xs">
              <span>⚠️</span>
              <span>No worker nodes running. Start a worker with: <code class="bg-slate-800 px-2 py-0.5 rounded text-indigo-300">python -m src.worker</code></span>
            </span>
          </td></tr>`;
        } else {
          workersBody.innerHTML = workers.map(w => {
            const usagePct = w.concurrency > 0 ? Math.round((w.active_tasks / w.concurrency) * 100) : 0;
            const barColor = usagePct > 80 ? 'bg-rose-500' : (usagePct > 40 ? 'bg-amber-500' : 'bg-emerald-500');
            const queuesBadges = w.queues.map(q => `<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-300 text-[11px] font-mono mr-1">${q}</span>`).join('');
            return `
              <tr class="hover:bg-slate-800/30 transition">
                <td class="px-5 py-3 font-medium text-slate-200">
                  <div class="flex items-center space-x-2">
                    <span class="relative flex h-2 w-2">
                      <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                      <span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                    </span>
                    <span class="font-mono text-xs text-indigo-300">${w.worker_id}</span>
                  </div>
                </td>
                <td class="px-5 py-3 text-slate-400 text-xs">${w.hostname} (PID: ${w.pid})</td>
                <td class="px-5 py-3">${queuesBadges}</td>
                <td class="px-5 py-3 text-center">
                  <div class="inline-flex flex-col items-center w-36">
                    <span class="text-xs font-semibold text-slate-300">${w.active_tasks} / ${w.concurrency} (${usagePct}%)</span>
                    <div class="w-full bg-slate-800 rounded-full h-1.5 mt-1 overflow-hidden">
                      <div class="${barColor} h-1.5 rounded-full" style="width: ${Math.max(4, usagePct)}%"></div>
                    </div>
                  </div>
                </td>
                <td class="px-5 py-3 text-right text-xs text-slate-400">${Math.round(w.uptime_seconds)}s</td>
              </tr>
            `;
          }).join('');
        }

        // Queues Table
        const queuesList = Object.entries(data.queues || {});
        document.getElementById('queues-count-badge').innerText = `${queuesList.length} queues`;

        const queuesBody = document.getElementById('queues-table-body');
        if (queuesList.length === 0) {
          queuesBody.innerHTML = '<tr><td colspan="5" class="px-5 py-6 text-center text-slate-500">No active queues</td></tr>';
        } else {
          queuesBody.innerHTML = queuesList.map(([name, q]) => `
            <tr class="hover:bg-slate-800/30 transition">
              <td class="px-5 py-3 font-medium text-slate-200">
                <span class="inline-flex items-center space-x-1.5">
                  <span class="h-2 w-2 rounded-full bg-indigo-400"></span>
                  <span>${name}</span>
                </span>
              </td>
              <td class="px-5 py-3 text-center">
                <span class="px-2 py-0.5 rounded text-xs font-semibold ${q.pending > 0 ? 'bg-indigo-950/80 text-indigo-300 border border-indigo-800' : 'text-slate-400'}">${q.pending}</span>
              </td>
              <td class="px-5 py-3 text-center">
                <span class="px-2 py-0.5 rounded text-xs font-semibold ${q.delayed > 0 ? 'bg-amber-950/80 text-amber-300 border border-amber-800' : 'text-slate-400'}">${q.delayed}</span>
              </td>
              <td class="px-5 py-3 text-center">
                <span class="px-2 py-0.5 rounded text-xs font-semibold ${q.failed > 0 ? 'bg-rose-950/80 text-rose-300 border border-rose-800' : 'text-slate-400'}">${q.failed}</span>
              </td>
              <td class="px-5 py-3 text-right">
                ${q.failed > 0 ? `
                  <button onclick="clearDLQ('${name}')" class="text-xs px-2.5 py-1 rounded bg-rose-600/20 hover:bg-rose-600/40 text-rose-300 border border-rose-700/50 transition">
                    Clear DLQ
                  </button>
                ` : `<span class="text-xs text-slate-500">Healthy</span>`}
              </td>
            </tr>
          `).join('');
        }

        // Schedules Table
        const schedBody = document.getElementById('schedules-table-body');
        const schedules = data.schedules || [];
        if (schedules.length === 0) {
          schedBody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">No active schedules</td></tr>';
        } else {
          schedBody.innerHTML = schedules.map(s => {
            const nextDate = new Date(s.next_execution * 1000);
            return `
              <tr class="hover:bg-slate-800/30 transition text-xs">
                <td class="px-5 py-2.5 font-medium text-slate-200">${s.task_name}</td>
                <td class="px-5 py-2.5 font-mono text-indigo-300">${s.cron_expression}</td>
                <td class="px-5 py-2.5 text-slate-400">${s.queue_name}</td>
                <td class="px-5 py-2.5 text-right text-slate-400">${nextDate.toLocaleTimeString()}</td>
              </tr>
            `;
          }).join('');
        }

        // History Table
        const histBody = document.getElementById('history-table-body');
        const history = data.recent_history || [];
        if (history.length === 0) {
          histBody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">No task history recorded yet</td></tr>';
        } else {
          histBody.innerHTML = history.map(h => {
            const isSuccess = h.status === 'SUCCESS';
            const badgeClass = isSuccess ? 'bg-emerald-950/80 text-emerald-300 border-emerald-800' : 'bg-rose-950/80 text-rose-300 border-rose-800';
            const completed = h.completed_at ? new Date(h.completed_at).toLocaleTimeString() : 'N/A';
            return `
              <tr class="hover:bg-slate-800/30 transition text-xs">
                <td class="px-5 py-2.5 font-medium text-slate-200">${h.task_name}</td>
                <td class="px-5 py-2.5">
                  <span class="px-1.5 py-0.5 rounded border text-[10px] font-semibold ${badgeClass}">${h.status}</span>
                </td>
                <td class="px-5 py-2.5 text-slate-400">${h.queue_name}</td>
                <td class="px-5 py-2.5 text-right text-slate-400">${completed}</td>
              </tr>
            `;
          }).join('');
        }

      } catch (e) {
        console.error('Error fetching dashboard stats:', e);
      }
    }

    // Auto poll every 3 seconds
    fetchStats();
    setInterval(fetchStats, 3000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)
