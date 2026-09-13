use async_nats::Client;
use chrono::Utc;
use sqlx::PgPool;

use crate::models::Cron;
use crate::models::Task;
use crate::models::TaskWithCron;

/// The parser both the scheduler and its tests read patterns with.
///
/// `sloppy_ranges` keeps `0/7` and `/7` parsing: croner 4 refuses them as
/// non-standard, and a cron already in the database would stop firing.
fn parser() -> croner::parser::CronParser {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .dom_and_dow(true)
        .sloppy_ranges(true)
        .build()
}

pub async fn run_scheduled_tasks(pool: &PgPool, nats: &Client) -> Result<(), sqlx::Error> {
    log::debug!("Running scheduled tasks");

    let query = "SELECT * FROM crons WHERE next_run <= NOW()";
    let crons = sqlx::query_as::<_, Cron>(query).fetch_all(pool).await?;

    log::info!("Found {} scheduled tasks", crons.len());

    // Set next run time for each cron amd map into task
    let tasks = {
        let mut tasks = Vec::new();

        for cron in crons {
            log::debug!("Calculating next run time for cron: {:?}", cron);

            match parser().parse(&cron.value) {
                Err(_) => {
                    log::error!("Invalid cron expression: {}", cron.value);
                    continue;
                }
                Ok(schedule) => {
                    let next = match schedule.find_next_occurrence(&Utc::now(), false) {
                        Ok(time) => time,
                        Err(err) => {
                            log::error!(
                                "Failed to calculate next run time for cron: {} {}",
                                cron.value,
                                err
                            );
                            continue;
                        }
                    };

                    log::debug!("Next run time: {}", next);

                    {
                        let query = "UPDATE crons SET next_run = $1, last_run = $2 WHERE id = $3";
                        let _ = sqlx::query(query)
                            .bind(next)
                            .bind(cron.next_run)
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
        log::info!(
            "Executing scheduled task: {} (cron {})",
            task.id(),
            task.cron_id()
        );

        let payload = task.to_json();

        log::debug!("Publishing task: {}", payload);

        if let Err(err) = nats.publish("scheduled", payload.to_string().into()).await {
            log::error!("Failed to publish task: {}", err);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parser;
    use chrono::Timelike;

    fn cron(pattern: &str) -> Result<croner::Cron, croner::errors::CronError> {
        parser().parse(pattern)
    }

    #[tokio::test]
    async fn test_cron_schedule() {
        let next = cron("0 * * * *").unwrap();

        let now = chrono::Utc::now();
        let next = next.find_next_occurrence(&now, false).unwrap();

        assert_eq!(next.minute(), 0);
    }

    #[tokio::test]
    async fn test_cron_parser() {
        assert!(cron("0 * * * *").is_ok()); // Every hour
        assert!(cron("0 0 * * * *").is_ok()); // Every hour
        assert!(cron("0 * * * * *").is_ok()); // Every minute
        assert!(cron("* * * * * *").is_ok()); // Every second
        assert!(cron("/7 * * * * *").is_ok()); // Every 7 seconds
        assert!(cron("*/7 * * * * *").is_ok()); // Every 7 seconds
        assert!(cron("0/7 * * * * *").is_ok()); // Every 7 seconds
        assert!(cron("1/7 * * * * *").is_ok()); // Every 7 seconds, starting at 1 seconds past the minute
        assert!(cron("*/7 * * * *").is_ok()); // Every 7 minutes
        assert!(cron("0/7 * * * *").is_ok()); // Every 7 minutes
        assert!(cron("1/7 * * * *").is_ok()); // Every 7 minutes, starting at 1 minutes past the hour

        assert!(cron("0/1 * * * *").is_ok());
        assert!(cron("*/5 * * * *").is_ok());
        assert!(cron("0 * * * *").is_ok());

        assert!(cron("*/5 * * * * *").is_ok());
    }
}
