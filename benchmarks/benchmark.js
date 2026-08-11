import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const submitTaskLatency = new Trend('submit_task_latency');
const listTasksLatency = new Trend('list_tasks_latency');
const errorRate = new Rate('error_rate');

export const options = {
  stages: [
    { duration: '10s', target: 50 }, // Ramp up to 50 users
    { duration: '30s', target: 50 }, // Stay at 50 users for 30 seconds
    { duration: '10s', target: 0 },  // Ramp down to 0 users
  ],
  thresholds: {
    'http_req_duration': ['p(95)<500'], // 95% of requests must complete below 500ms
    'error_rate': ['rate<0.01'],        // Error rate must be less than 1%
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8000';

export default function () {
  // 1. Submit a task
  const submitPayload = JSON.stringify({
    payload: {
      job_type: 'benchmark',
      timestamp: Date.now()
    }
  });
  
  const submitParams = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const submitRes = http.post(`${BASE_URL}/tasks/`, submitPayload, submitParams);
  
  submitTaskLatency.add(submitRes.timings.duration);
  
  const isSubmitSuccessful = check(submitRes, {
    'submit status is 200': (r) => r.status === 200,
    'has task id': (r) => r.json('id') !== undefined,
  });
  
  errorRate.add(!isSubmitSuccessful);

  sleep(0.5); // Simulate user think time

  // 2. List tasks
  const listRes = http.get(`${BASE_URL}/tasks/?limit=10`);
  
  listTasksLatency.add(listRes.timings.duration);
  
  const isListSuccessful = check(listRes, {
    'list status is 200': (r) => r.status === 200,
  });
  
  errorRate.add(!isListSuccessful);
  
  sleep(0.5);
}
