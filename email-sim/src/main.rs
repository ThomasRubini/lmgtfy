use clap::Parser;
use common::{
    EMAIL_MSG_QUEUE,
    dto::EmailMessage,
    queue::{QueueManager, pgmq::PgMqQueueManager},
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

    let queue_mgr = PgMqQueueManager::new()
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    queue_mgr
        .create(EMAIL_MSG_QUEUE)
        .await
        .expect("Failed to create queue");

    for _ in 0..args.count {
        // Create a message
        let msg = EmailMessage {
            from: "from".to_string(),
            to: "to".to_string(),
            content: "content".to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Send the message
        let msg_id: i64 = queue_mgr
            .send(EMAIL_MSG_QUEUE, &msg)
            .await
            .expect("Failed to enqueue message");
        println!("Sent a message (id={}): {:?}", msg_id, msg);
    }

    Ok(())
}
