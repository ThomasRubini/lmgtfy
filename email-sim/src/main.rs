use clap::Parser;
use common::{
    EMAIL_MSG_QUEUE,
    dto::EmailMessage,
    queue::{QueueManager, kafka::KafkaQueueManager},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command()]
struct Args {
    /// Number of messages to send
    #[arg(long)]
    count: Option<u32>,

    /// Run in loop, sending one message every second
    #[arg(long)]
    loop_send: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Connecting to Kafka for sending messages...");
    let queue_mgr = KafkaQueueManager::new()
        .await
        .expect("Failed to connect to Kafka - ensure Kafka is running on localhost:9092");

    // Create topic if it doesn't exist
    queue_mgr
        .create(EMAIL_MSG_QUEUE)
        .await
        .expect(&format!("Failed to create topic '{}'", EMAIL_MSG_QUEUE));
    println!("Topic '{}' ready", EMAIL_MSG_QUEUE);

    if args.loop_send {
        let mut i: u32 = 1;
        loop {
            let msg = EmailMessage {
                from: format!("user{}@example.com", i),
                to: "support@company.com".to_string(),
                content: format!("Hello, this is message #{} - I need help!", i),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to get current timestamp")
                    .as_secs(),
            };

            let msg_id = queue_mgr
                .send(EMAIL_MSG_QUEUE, &msg)
                .await
                .expect(&format!("Failed to send message {} to Kafka", i));
            
            println!("Sent message {} (id={}): from={}, content={}", 
                     i, msg_id, msg.from, msg.content);
            
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            i += 1;
        }
    } else if let Some(count) = args.count {
        for i in 1..=count {
            let msg = EmailMessage {
                from: format!("user{}@example.com", i),
                to: "support@company.com".to_string(),
                content: format!("Hello, this is message #{} - I need help!", i),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to get current timestamp")
                    .as_secs(),
            };

            let msg_id = queue_mgr
                .send(EMAIL_MSG_QUEUE, &msg)
                .await
                .expect(&format!("Failed to send message {} to Kafka", i));
            
            println!("Sent message {}/{} (id={}): from={}, content={}", 
                     i, count, msg_id, msg.from, msg.content);
        }

        println!("All {} messages sent successfully!", count);
    } else {
        println!("Error: Please specify --count or --loop-send");
    }
    Ok(())
}
