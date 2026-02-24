use anyhow::Result;
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    message::Message as KafkaMessage,
    util::Timeout,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;
use futures::StreamExt;

use crate::{
    KAFKA_BOOTSTRAP_SERVERS,
    queue::{Message, QueueManager},
};

pub struct KafkaQueueManager {
    producer: FutureProducer,
    consumer: StreamConsumer,
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
}

impl KafkaQueueManager {
    pub async fn new() -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .set("message.timeout.ms", "5000")
            .create()?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .set("group.id", "lmgtfy-consumer-group")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        let admin_client: AdminClient<rdkafka::client::DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .create()?;

        Ok(KafkaQueueManager {
            producer,
            consumer,
            admin_client,
        })
    }
}

impl QueueManager for KafkaQueueManager {
    async fn create(&self, queue_name: &str) -> Result<()> {
        let topic = NewTopic::new(queue_name, 1, TopicReplication::Fixed(1));
        let dlq_name = format!("{}_dlq", queue_name);
        let dlq_topic = NewTopic::new(&dlq_name, 1, TopicReplication::Fixed(1));
        
        let topics = vec![topic, dlq_topic];
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
        
        let results = self.admin_client.create_topics(&topics, &opts).await?;
        
        for result in results {
            match result {
                Ok(_) => {} // Topic created successfully
                Err((topic_name, error)) => {
                    // Ignore "topic already exists" errors
                    if !error.to_string().contains("already exists") {
                        return Err(anyhow::anyhow!("Failed to create topic {}: {}", topic_name, error));
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn send(&self, queue_name: &str, message: &impl Serialize) -> Result<i64> {
        let message_json = serde_json::to_string(message)?;
        let message_id = Uuid::new_v4().to_string();
        
        let record = FutureRecord::to(queue_name)
            .key(&message_id)
            .payload(&message_json);

        let (_partition, offset) = self.producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(kafka_error, _)| anyhow::anyhow!("Failed to send message: {}", kafka_error))?;

        // Return the offset as message ID (similar to PGMQ's message ID)
        Ok(offset)
    }

    async fn read<T: for<'de> Deserialize<'de> + Serialize>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>> {
        // Subscribe to the topic if not already subscribed
        self.consumer.subscribe(&[queue_name])?;

        // Try to get a message with a short timeout
        let message_stream = self.consumer.stream();
        let mut stream = Box::pin(message_stream);
        
        // Use a timeout to avoid blocking indefinitely
        match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(kafka_msg))) => {
                let payload = match kafka_msg.payload() {
                    Some(payload) => payload,
                    None => return Ok(None),
                };

                let message_str = std::str::from_utf8(payload)?;
                let message: T = serde_json::from_str(message_str)?;
                
                // Use offset as message ID for Kafka
                let msg_id = kafka_msg.offset();

                // Check retry count (we'll store it in headers or use a different approach)
                // For now, we'll implement basic functionality without retry counting
                // TODO: Implement retry counting similar to PGMQ

                Ok(Some(Message { msg_id, message }))
            }
            Ok(Some(Err(e))) => Err(anyhow::anyhow!("Kafka error: {}", e)),
            Ok(None) | Err(_) => Ok(None), // Timeout or no message
        }
    }

    async fn delete(&self, _queue_name: &str, _message_id: i64) -> Result<()> {
        // In Kafka, we commit the offset to "delete" (acknowledge) the message
        self.consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }
}