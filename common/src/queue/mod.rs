use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::EMAIL_MSG_QUEUE;

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

    /// Receive a message from the queue with a given visibility timeout
    /// Returns (message_id, message_content)
    async fn read<T: for<'de> Deserialize<'de> + Serialize>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>>;

    /// Delete/acknowledge a message from the queue
    async fn delete(&self, queue_name: &str, message_id: i64) -> Result<()>;

    async fn register_read<T: for<'de> Deserialize<'de> + Serialize, R>(
        &self,
        queue_name: &str,
        process: &dyn Fn(Message<T>) -> R,
    ) -> Result<()>
    where
        R: Future<Output = anyhow::Result<()>>,
    {
        // Infinite loop for reading messages
        loop {
            // Read a message
            let received_msg: Message<T> = match self.read(EMAIL_MSG_QUEUE).await.unwrap() {
                Some(msg) => msg,
                None => {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            println!("Received a message (id={})", received_msg.msg_id);

            // Process the message
            let msg_id = received_msg.msg_id;
            match process(received_msg).await {
                Ok(()) => {
                    // If processing is successful, delete the message from the queue
                    self.delete(queue_name, msg_id).await?;
                }
                Err(err) => {
                    println!("Error processing message {}: {}", msg_id, err);
                    // If processing fails, do nothing, the message will be retired again
                }
            }
        }
    }
}
