# OpenWorkers Scheduler

> **Archived.** This repository is read-only.
>
> [openworkers-runner](https://github.com/openworkers/openworkers-runner) v1.15.0
> and later start the crons. Do not run this service on the database of such a
> runner. Use it only with runner 0.15.x.

Cron task scheduler for OpenWorkers.

Uses NATS to dispatch scheduled tasks to [openworkers-runner](https://github.com/openworkers/openworkers-runner).

## Configuration

Required environment variables:

```bash
DATABASE_URL=postgresql://user:password@localhost/db
NATS_SERVERS=nats://localhost:4222
```

## Usage

```bash
cargo run
```

## Stack

- Rust + Tokio (async runtime)
- PostgreSQL (cron storage)
- NATS (message broker)
