# PyFlow Benchmarking

This directory contains scripts to benchmark the PyFlow API using [k6](https://k6.io/), measuring throughput, latency, and error rates.

## Prerequisites

1. **Install k6**:
   - **macOS**: `brew install k6`
   - **Linux (Debian/Ubuntu)**: 
     ```bash
     sudo gpg -k
     sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
     echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
     sudo apt-get update
     sudo apt-get install k6
     ```
   - **Windows**: `winget install k6` or download the MSI from their releases page.

2. **Start the Application**:
   Ensure PyFlow and its Redis dependency are running.
   ```bash
   # If using docker-compose
   docker-compose up -d
   
   # OR running locally
   uvicorn src.main:app --host 0.0.0.0 --port 8000
   ```

## Running the Benchmark

The `benchmark.js` script ramps up to 50 concurrent virtual users over 10 seconds, sustains that load for 30 seconds, and scales back down over 10 seconds. 

To run it, execute the following command:

```bash
k6 run benchmarks/benchmark.js
```

If your API is running on a different port or host, pass the `API_URL` environment variable:

```bash
k6 run -e API_URL=http://your-production-url:8000 benchmarks/benchmark.js
```

## Interpreting Results

K6 will output a summary to your terminal. Key metrics to observe:

- **`http_req_duration`**: The end-to-end latency of your HTTP requests (look at the `p(95)` for the 95th percentile latency).
- **`http_reqs`**: The total throughput (number of requests processed) and requests per second (`/s`).
- **`submit_task_latency`** & **`list_tasks_latency`**: Custom metrics showing the latency for the specific endpoints.
- **`error_rate`**: Should ideally be `0.00%`. If it starts increasing, your server is rejecting requests (e.g., due to rate limiting or crashes).

### Recording Results

To record results in a structured format for historical tracking, you can output the benchmark summary to a JSON or CSV file:

```bash
# Output to JSON
k6 run --out json=benchmarks/results.json benchmarks/benchmark.js

# Output to CSV
k6 run --out csv=benchmarks/results.csv benchmarks/benchmark.js
```

You can also use k6 Cloud or integrate with Prometheus/Grafana or Datadog for visual dashboards. See the [k6 output documentation](https://k6.io/docs/results-output/) for more information.
