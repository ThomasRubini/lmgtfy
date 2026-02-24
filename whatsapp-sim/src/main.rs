use clap::Parser;
use common::{
    WHATSAPP_MSG_QUEUE,
    dto::WhatsAppMessage,
    queue::{QueueManager, kafka::KafkaQueueManager},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command()]
struct Args {
    /// Number of messages to send
    #[arg(long)]
    count: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Connecting to Kafka for sending WhatsApp messages...");
    let queue_mgr = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");

    // Create topic if it doesn't exist
    queue_mgr
        .create(WHATSAPP_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create topic '{}'", WHATSAPP_MSG_QUEUE));
    println!("Topic '{}' ready", WHATSAPP_MSG_QUEUE);

    for i in 1..=args.count {
        let msg = WhatsAppMessage {
            sender: format!("+336{:02}123456", i),
            content: format!("Hello, this is WhatsApp message #{} - I need help!", i),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get current timestamp")
                .as_secs(),
        };

        let msg_id = queue_mgr
            .send(WHATSAPP_MSG_QUEUE, &msg)
            .await
            .expect(&format!("Failed to send WhatsApp message {} to Kafka", i));
        
        println!("Sent message {}/{} (id={}): from={}, content={}", 
                 i, args.count, msg_id, msg.sender, msg.content);
    }

    println!("All {} WhatsApp messages sent successfully!", args.count);
    Ok(())
}
