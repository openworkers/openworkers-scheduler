use std::str;

use serde::Deserialize;
use serde::Serialize;
use sqlx::prelude::FromRow;
use sqlx::types::Uuid;

pub type UtcDateTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
#[sqlx(type_name = "scheduled_events")]
pub struct Task {
    pub id: Uuid,
    pub cron_id: Uuid,
    pub worker_id: Uuid,
    pub replied_at: Option<UtcDateTime>,
    pub executed_at: UtcDateTime,
    pub scheduled_at: UtcDateTime,
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

    pub fn id(&self) -> String {
        self.task.id.to_string()
    }

    pub fn worker_id(&self) -> String {
        self.task.worker_id.to_string()
    }

    pub fn cron_id(&self) -> String {
        self.task.cron_id.to_string()
    }

    pub fn scheduled_at(&self) -> UtcDateTime {
        self.task.scheduled_at
    }

    pub fn cron_value(&self) -> String {
        self.cron.value.to_string()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.cron_id(),
            "workerId": self.worker_id(),
            "cron": self.cron_value(),
            "scheduledTime": self.scheduled_at().timestamp_millis()
        })
    }
}

#[derive(Debug, Deserialize, FromRow)]
#[sqlx(type_name = "crons")]
#[allow(dead_code)]
pub struct Cron {
    pub id: Uuid,
    pub value: String,
    pub worker_id: Uuid,
    pub last_run: Option<UtcDateTime>,
    pub next_run: Option<UtcDateTime>,
    pub created_at: UtcDateTime,
    pub updated_at: UtcDateTime,
    pub deleted_at: Option<UtcDateTime>,
}
