use dotenv::dotenv;
use nats::nats_connect;
use sqlx::PgPool;
use tokio::time::interval;
use tokio::time::Duration;
use std::env;

mod models;
mod nats;
mod scheduler;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let interval_secs: u64 = env::var("CHECK_INTERVAL")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .expect("CHECK_INTERVAL must be a number");

    // Connect to the database
    let pool = PgPool::connect(&db_url).await.expect("Failed to connect to DB");
    log::info!("Connected to the database");

    // Connect to NATS
    let nats_client = nats_connect().await.expect("Failed to connect to NATS");
    log::info!("Connected to NATS server");

    // Run the scheduler
    let mut interval = interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        match scheduler::run_scheduled_tasks(&pool, &nats_client).await {
            Ok(_) => log::info!("Scheduled tasks executed successfully"),
            Err(e) => log::error!("Error executing scheduled tasks: {:?}", e),
        }
    }
}
