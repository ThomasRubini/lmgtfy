use anyhow::Result;
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    message::Message as KafkaMessage,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

use crate::{
    queue::{get_dlq_name, Message, QueueManager},
    KAFKA_BOOTSTRAP_SERVERS, MAX_RETRIES,
};

pub struct KafkaQueueManager {
    producer: FutureProducer,
    consumer: StreamConsumer,
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
    seen_messages: HashMap<String, usize>,
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
            seen_messages: HashMap::new(),
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
                        return Err(anyhow::anyhow!(
                            "Failed to create topic {}: {}",
                            topic_name,
                            error
                        ));
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

        let (_partition, offset) = self
            .producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(kafka_error, _)| {
                anyhow::anyhow!("Failed to send message: {}", kafka_error)
            })?;

        // Return the offset as message ID (similar to PGMQ's message ID)
        Ok(offset)
    }

    async fn register_read<T: for<'de> Deserialize<'de> + Serialize, R>(
        &mut self,
        queue_name: &str,
        process: &dyn Fn(Message<T>) -> R,
    ) -> Result<()>
    where
        R: std::prelude::rust_2024::Future<Output = anyhow::Result<()>>,
    {
        self.consumer.subscribe(&[queue_name])?;

        // Try to get a message with a short timeout
        let message_stream = self.consumer.stream();
        let mut stream = Box::pin(message_stream);

        while let Some(message_result) = stream.next().await {
            match message_result {
                Ok(kafka_msg) => {
                    let payload = match kafka_msg.payload() {
                        Some(payload) => payload,
                        None => continue,
                    };
                    println!("Received a message (id={})", kafka_msg.offset());

                    let message_str = std::str::from_utf8(payload)?;
                    let message: T = serde_json::from_str(message_str)?;

                    // Use offset as message ID for Kafka
                    let msg_id = kafka_msg.offset();

                    let seen = self
                        .seen_messages
                        .entry(msg_id.to_string())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    if *seen > MAX_RETRIES as usize {
                        // Move message to DLQ
                        println!(
                            "Message with ID {} has been seen {} times, moving to DLQ",
                            msg_id, seen
                        );
                        let dlq_name = get_dlq_name(queue_name);
                        self.send(&dlq_name, &message).await?;

                        // Commit the offset to skip the message
                        self.consumer
                            .commit_message(&kafka_msg, rdkafka::consumer::CommitMode::Async)?;

                        // Reset seen count for this queue
                        self.seen_messages.remove(queue_name);

                        continue;
                    }

                    let msg = Message { msg_id, message };
                    if let Err(e) = process(msg).await {
                        eprintln!("Error processing message: {}", e);
                    } else {
                        // If processed successfully, commit the offset to delete the message
                        self.consumer
                            .commit_message(&kafka_msg, rdkafka::consumer::CommitMode::Async)?;
                    }
                }
                Err(e) => eprintln!("Kafka error: {}", e),
            }
        }
        Err(anyhow::anyhow!("Message stream ended unexpectedly"))
    }

    async fn delete(&self, _queue_name: &str, _message_id: i64) -> Result<()> {
        // In Kafka, we commit the offset to "delete" (acknowledge) the message
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }
}
