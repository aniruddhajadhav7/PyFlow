# PyFlow

A production-style Python backend project using FastAPI.

## Features
- FastAPI for high-performance API development
- Pydantic Settings for environment variable management
- Structlog for structured JSON logging
- Modular directory structure

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the application:
   ```bash
   uvicorn src.main:app --reload
   ```


## Testing

To run the PyTest suite covering API endpoints, Redis queues, workers, and rate limiting:

1. Install test dependencies:
   ```bash
   pip install -r requirements-test.txt
   ```

2. Run tests:
   ```bash
   pytest -v tests/
   ```

## Benchmarking

A [k6](https://k6.io/) benchmarking script is provided to measure throughput, latency, and error rates of the application's endpoints.

See the documentation in `benchmarks/README.md` for instructions on how to install k6, run the benchmarks, and record the results.
