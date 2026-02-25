use common::{
    COMMON_MSG_QUEUE, WHATSAPP_MSG_QUEUE,
    dto::{CommonMessage, WhatsAppMessage},
    queue::{QueueManager, kafka::KafkaQueueManager, Message},
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting WhatsApp processor with Kafka...");
    let mut queue_mgr = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");

    // Create queues
    queue_mgr
        .create(WHATSAPP_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create WhatsApp topic '{}'", WHATSAPP_MSG_QUEUE));
    queue_mgr
        .create(COMMON_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create common topic '{}'", COMMON_MSG_QUEUE));

    println!("Listening for WhatsApp messages on topic '{}'...", WHATSAPP_MSG_QUEUE);
    
    loop {
        match queue_mgr.read::<WhatsAppMessage>(WHATSAPP_MSG_QUEUE).await {
            Ok(Some(Message { msg_id, message })) => {
                println!("Received WhatsApp message (id={}): {:?}", msg_id, message);
                
                // Transform to common message
                let common_msg: CommonMessage = message.into();
                println!("Transformed to: {:?}", common_msg);
                
                // Send to common queue
                let forwarded_id = queue_mgr
                    .send(COMMON_MSG_QUEUE, &common_msg)
                    .await
                    .expect(&format!("Failed to forward message {} to common queue", msg_id));
                println!("Forwarded to common queue (id={})", forwarded_id);
                
                // Acknowledge the original message
                queue_mgr
                    .delete(WHATSAPP_MSG_QUEUE, msg_id)
                    .await
                    .expect(&format!("Failed to acknowledge message {}", msg_id));
                println!("Original message acknowledged");
            }
            Ok(None) => {
                // No messages available, wait a bit
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                eprintln!("Error reading from WhatsApp queue: {}", e);
                eprintln!("Retrying in 2 seconds...");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}