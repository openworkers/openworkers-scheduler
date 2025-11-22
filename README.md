# OpenWorkers Scheduler

Cron task scheduler for OpenWorkers.

Uses NATS to dispatch scheduled tasks to [openworkers-runner](https://github.com/openworkers/openworkers-runner).

## Configuration

Required environment variables:

```bash
DATABASE_URL=postgresql://user:password@localhost/db
NATS_URL=nats://localhost:4222
```

## Usage

```bash
cargo run
```

## Stack

- Rust + Tokio (async runtime)
- PostgreSQL (cron storage)
- NATS (message broker)
