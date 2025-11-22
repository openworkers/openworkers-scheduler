use dotenv::dotenv;
use nats::nats_connect;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::env;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio::time::Duration;

mod models;
mod nats;
mod scheduler;

use models::UtcDateTime;

async fn get_next_run(pool: &PgPool) -> Option<UtcDateTime> {
    let value = sqlx::query_scalar!("SELECT next_run FROM crons ORDER BY next_run ASC LIMIT 1")
        .fetch_one(pool)
        .await
        .expect("Failed to fetch next run time")?;

    UtcDateTime::from_timestamp(value.unix_timestamp(), 0)
}

async fn get_next_duration(pool: &PgPool) -> Duration {
    let next_run = get_next_run(pool).await;

    let time = match next_run {
        Some(time) => time,
        None => return Duration::from_secs(10),
    };

    let duration = time - chrono::Utc::now();
    log::info!("Next run in: {:?}", duration);

    if duration > chrono::Duration::zero() {
        duration.to_std().expect("Failed to convert duration")
    } else {
        Duration::from_secs(1)
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Connect to the database
    let pool = PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    log::info!("Connected to the database");

    // Connect to NATS
    let nats_client = nats_connect().await.expect("Failed to connect to NATS");
    log::info!("Connected to NATS server");

    // Create a notification channel
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // Start listening to Postgres notifications
    let mut listener = PgListener::connect_with(&pool)
        .await
        .expect("Failed to create listener");

    listener
        .listen("cron_update")
        .await
        .expect("Failed to listen to cron updates");

    tokio::spawn(async move {
        loop {
            let notification = listener
                .recv()
                .await
                .expect("Failed to receive notification");

            log::info!("Received notification: {:?}", notification);

            notify_clone.notify_one();
        }
    });

    // Run the scheduler loop
    let mut skip_exec = false; // Skip execution if notification is received
    loop {
        let duration = match skip_exec {
            true => get_next_duration(&pool).await,
            false => match scheduler::run_scheduled_tasks(&pool, &nats_client).await {
                Ok(_) => get_next_duration(&pool).await,
                Err(e) => {
                    log::error!("Error executing scheduled tasks: {:?}", e);
                    Duration::from_secs(10)
                }
            },
        };

        skip_exec = false;
        log::info!("Sleeping for {} seconds", duration.as_secs());

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Shutting down gracefully...");
                break;
            },
            _ = sleep(duration) => {},
            _ = notify.notified() => {
                log::info!("Notification received, recalculating next run time");
                skip_exec = true;
            }
        }
    }
}
