use std::future::Future;

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub mod kafka;
pub mod pgmq;

pub struct Message<T> {
    pub msg_id: i64,
    pub message: T,
}

fn get_dlq_name(queue_name: &str) -> String {
    format!("{}_dlq", queue_name)
}

/// Abstraction over a queue, automatically move messages to a DLQ after a certain number of retries
#[allow(async_fn_in_trait)]
pub trait QueueManager: Send + Sync {
    async fn create(&self, queue_name: &str) -> Result<()>;

    /// Send a message to the queue
    async fn send(&self, queue_name: &str, message: &impl Serialize) -> Result<i64>;

    /// Delete/acknowledge a message from the queue
    async fn delete(&self, queue_name: &str, message_id: i64) -> Result<()>;

    async fn register_read<T: for<'de> Deserialize<'de> + Serialize, R>(
        &mut self,
        queue_name: &str,
        process: &dyn Fn(Message<T>) -> R,
    ) -> Result<()>
    where
        R: Future<Output = anyhow::Result<()>>;
}
