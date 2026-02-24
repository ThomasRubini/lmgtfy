use anyhow::Result;
use serde::{Deserialize, Serialize};

pub mod kafka;
pub mod pgmq;

pub struct Message<T> {
    pub msg_id: i64,
    pub message: T,
}

#[allow(async_fn_in_trait)]
pub trait QueueManager: Send + Sync {
    async fn create(&self, queue_name: &str) -> Result<()>;

    /// Send a message to the queue
    async fn send(&self, queue_name: &str, message: &impl Serialize) -> Result<()>;

    /// Receive a message from the queue with a given visibility timeout
    /// Returns (message_id, message_content)
    async fn read<T: for<'de> Deserialize<'de> + Serialize>(&self, queue_name: &str) -> Result<Option<Message<T>>>;

    /// Delete/acknowledge a message from the queue
    async fn delete(&self, queue_name: &str, message_id: i64) -> Result<()>;
}
