use anyhow::Context;
use common::{
    dto::{CommonMessage, WhatsAppMessage},
    queue::{kafka::KafkaQueueManager, Message, QueueManager},
    COMMON_MSG_QUEUE, WHATSAPP_MSG_QUEUE,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting WhatsApp processor with Kafka...");
    let mut queue_mgr = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");

    // Create queues
    queue_mgr.create(WHATSAPP_MSG_QUEUE).await.expect(&format!(
        "Failed to create WhatsApp topic '{}'",
        WHATSAPP_MSG_QUEUE
    ));
    queue_mgr.create(COMMON_MSG_QUEUE).await.expect(&format!(
        "Failed to create common topic '{}'",
        COMMON_MSG_QUEUE
    ));

    println!(
        "Listening for WhatsApp messages on topic '{}'...",
        WHATSAPP_MSG_QUEUE
    );

    let queue_mgr2 = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");
    queue_mgr
        .register_read(WHATSAPP_MSG_QUEUE, &async |wrapper: Message<
            WhatsAppMessage,
        >| {
            // Transform to common message
            let common_msg: CommonMessage = wrapper.message.into();
            println!("Transformed to: {:?}", common_msg);

            // Send to common queue
            let forwarded_id = queue_mgr2
                .send(COMMON_MSG_QUEUE, &common_msg)
                .await
                .context("Failed to forward message to common queue")?;
            println!("Forwarded to common queue (id={})", forwarded_id);
            Ok(())
        })
        .await
}
