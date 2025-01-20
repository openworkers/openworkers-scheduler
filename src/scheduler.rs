use async_nats::Client;
use chrono::Utc;
use cron::Schedule;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use std::str::FromStr;

use crate::models::Cron;
use crate::models::Task;
use crate::models::TaskWithCron;

pub async fn run_scheduled_tasks(pool: &PgPool, nats: &Client) -> Result<(), sqlx::Error> {
    let query = "SELECT * FROM crons WHERE next_run <= NOW()";
    let crons = sqlx::query_as::<_, Cron>(query).fetch_all(pool).await?;

    log::info!("Found {} scheduled tasks", crons.len());

    // Set next run time for each cron amd map into task
    let tasks = {
        let mut tasks = Vec::new();

        for cron in crons {
            log::debug!("Calculating next run time for cron: {:?}", cron);

            match Schedule::from_str(&cron.value) {
                Err(_) => {
                    log::error!("Invalid cron expression: {}", cron.value);
                    continue;
                }
                Ok(schedule) => {
                    let next = match schedule.upcoming(Utc).next() {
                        Some(time) => time,
                        None => {
                            log::error!(
                                "Failed to calculate next run time for cron: {}",
                                cron.value
                            );
                            continue;
                        }
                    };

                    log::debug!("Next run time: {}", next);

                    let next = OffsetDateTime::from_unix_timestamp(next.timestamp()).unwrap();

                    {
                        let query = "UPDATE crons SET next_run = $1 WHERE id = $2";
                        let _ = sqlx::query(query)
                            .bind(next)
                            .bind(cron.id)
                            .execute(pool)
                            .await;
                    }

                    let query = r"
                        INSERT INTO scheduled_events (id, cron_id, worker_id, executed_at, scheduled_at) 
                        VALUES ($1, $2, $3, NOW(), $4) RETURNING *";
                    let task = sqlx::query_as::<_, Task>(query)
                        .bind(uuid::Uuid::new_v4())
                        .bind(cron.id)
                        .bind(cron.worker_id)
                        .bind(cron.next_run)
                        .fetch_one(pool)
                        .await;

                    log::debug!("Scheduled task: {:?}", task);

                    match task {
                        Ok(task) => tasks.push(TaskWithCron::new(task, cron)),
                        Err(err) => log::error!("Failed to create task: {}", err),
                    }
                }
            }
        }

        tasks
    };

    // Execute the scheduled tasks
    for task in tasks {
        log::info!("Executing scheduled task: {:?}", task);

        let payload = task.to_json();

        log::debug!("Publishing task: {}", payload);

        if let Err(err) = nats.publish("scheduled", payload.to_string().into()).await {
            log::error!("Failed to publish task: {}", err);
        }
    }

    Ok(())
}
