use std::{future::Future, time::Duration};

use pgmq::PGMQueueExt;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::{
    queue::{get_dlq_name, Message, QueueManager},
    MAX_RETRIES, PG_URL, VISIBILITY_TIMEOUT_SECONDS,
};

pub struct PgMqQueueManager {
    inner: PGMQueueExt,
}

impl PgMqQueueManager {
    pub async fn new() -> anyhow::Result<Self> {
        let inner = PGMQueueExt::new(PG_URL.to_string(), 1).await?;
        Ok(PgMqQueueManager { inner })
    }
}

impl QueueManager for PgMqQueueManager {
    async fn create(&self, queue_name: &str) -> anyhow::Result<()> {
        self.inner.create(queue_name).await?;
        let dlq_name = get_dlq_name(queue_name);
        self.inner.create(&dlq_name).await?;
        Ok(())
    }

    async fn send(&self, queue_name: &str, message: &impl Serialize) -> anyhow::Result<i64> {
        Ok(self.inner.send(queue_name, message).await?)
    }

    async fn register_read<T: for<'de> Deserialize<'de> + Serialize, R>(
        &mut self,
        queue_name: &str,
        process: &dyn Fn(Message<T>) -> R,
    ) -> anyhow::Result<()>
    where
        R: Future<Output = anyhow::Result<()>>,
    {
        // Infinite loop for reading messages
        loop {
            // Read a message
            let received_msg: Message<T> = match self.read(queue_name).await.unwrap() {
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

    async fn delete(&self, queue_name: &str, message_id: i64) -> anyhow::Result<()> {
        self.inner.delete(queue_name, message_id).await?;
        Ok(())
    }
}

impl PgMqQueueManager {
    async fn read<T: for<'de> Deserialize<'de> + Serialize>(
        &mut self,
        queue_name: &str,
    ) -> anyhow::Result<Option<Message<T>>> {
        let msg: Option<pgmq::Message<T>> = self
            .inner
            .read(queue_name, VISIBILITY_TIMEOUT_SECONDS)
            .await?;
        match msg {
            Some(m) => {
                if m.read_ct > MAX_RETRIES {
                    println!(
                        "Message {} has been read {} times, moving it to DLQ",
                        m.msg_id, m.read_ct
                    );
                    let dlq_name = get_dlq_name(queue_name);
                    self.inner.send(&dlq_name, &m.message).await?;
                    self.inner.delete(queue_name, m.msg_id).await?;
                    return Ok(None);
                }
                Ok(Some(Message {
                    msg_id: m.msg_id,
                    message: m.message,
                }))
            }
            None => Ok(None),
        }
    }
}
