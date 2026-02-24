use pgmq::PGMQueueExt;
use serde::{Deserialize, Serialize};

use crate::{
    MAX_RETRIES, PG_URL, VISIBILITY_TIMEOUT_SECONDS,
    queue::{Message, QueueManager},
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
        let dlq_name = format!("{}_dlq", queue_name);
        self.inner.create(&dlq_name).await?;
        Ok(())
    }

    async fn send(&self, queue_name: &str, message: &impl Serialize) -> anyhow::Result<i64> {
        Ok(self.inner.send(queue_name, message).await?)
    }

    async fn read<T: for<'de> Deserialize<'de> + Serialize>(
        &self,
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
                    let dlq_name = format!("{}_dlq", queue_name);
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

    async fn delete(&self, queue_name: &str, message_id: i64) -> anyhow::Result<()> {
        self.inner.delete(queue_name, message_id).await?;
        Ok(())
    }
}
