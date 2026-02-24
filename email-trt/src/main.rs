use clap::Parser;
use common::{
    COMMON_MSG_QUEUE, EMAIL_MSG_QUEUE,
    dto::{CommonMessage, EmailMessage},
    queue::{QueueManager, kafka::KafkaQueueManager, Message},
};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Parser)]
#[command()]
struct Args {
    /// Message content
    #[arg(long)]
    message: String,

    /// Number of messages to send
    #[arg(long)]
    count: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting email processor with Kafka...");
    let queue_mgr = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");

    // Create queues
    queue_mgr
        .create(EMAIL_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create email topic '{}'", EMAIL_MSG_QUEUE));
    queue_mgr
        .create(COMMON_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create common topic '{}'", COMMON_MSG_QUEUE));

    println!("Listening for email messages on topic '{}'...", EMAIL_MSG_QUEUE);
    
    loop {
        match queue_mgr.read::<EmailMessage>(EMAIL_MSG_QUEUE).await {
            Ok(Some(Message { msg_id, message })) => {
                println!("Received email message (id={}): {:?}", msg_id, message);
                
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
                    .delete(EMAIL_MSG_QUEUE, msg_id)
                    .await
                    .expect(&format!("Failed to acknowledge message {}", msg_id));
                println!("Original message acknowledged");
            }
            Ok(None) => {
                // No messages available, wait a bit
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                eprintln!("Error reading from email queue: {}", e);
                eprintln!("Retrying in 2 seconds...");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
