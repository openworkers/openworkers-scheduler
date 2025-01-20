use std::str;

use serde::Deserialize;
use serde::Serialize;
use sqlx::prelude::FromRow;
use sqlx::types::time::OffsetDateTime;
use sqlx::types::Uuid;

#[derive(Debug, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
#[sqlx(type_name = "scheduled_events")]
pub struct Task {
    pub id: Uuid,
    pub cron_id: Uuid,
    pub worker_id: Uuid,
    pub replied_at: Option<OffsetDateTime>,
    pub executed_at: OffsetDateTime,
    pub scheduled_at: OffsetDateTime,
}

#[derive(Debug)]
pub struct TaskWithCron {
    pub task: Task,
    pub cron: Cron,
}

impl TaskWithCron {
    pub fn new(task: Task, cron: Cron) -> Self {
        if task.cron_id != cron.id {
            panic!("Cron does not belong to task");
        }

        Self { task, cron }
    }

    pub fn worker_id(&self) -> String {
        self.task.worker_id.to_string()
    }

    pub fn cron_id(&self) -> String {
        self.task.cron_id.to_string()
    }

    pub fn scheduled_at(&self) -> OffsetDateTime {
        self.task.scheduled_at
    }

    pub fn cron_value(&self) -> String {
        self.cron.value.clone()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.cron_id(),
            "workerId": self.worker_id(),
            "cron": self.cron_value(),
            "scheduledTime": self.scheduled_at().unix_timestamp() * 1000
        })
    }
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
#[sqlx(type_name = "crons")]
pub struct Cron {
    pub id: Uuid,
    pub value: String,
    pub worker_id: Uuid,
    pub last_run: OffsetDateTime,
    pub next_run: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub deleted_at: Option<OffsetDateTime>,
}
