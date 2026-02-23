use clap::Parser;
use common::{EMAIL_MSG_QUEUE, PG_URL, dto::EmailMessage};
use pgmq::{PGMQueueExt, PgmqError};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command()]
struct Args {
    /// Number of messages to send
    #[arg(long)]
    count: u32,
}

#[tokio::main]
async fn main() -> Result<(), PgmqError> {
    let args = Args::parse();

    println!("Connecting to Postgres");
    let queue = PGMQueueExt::new(PG_URL.to_string(), 1)
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    queue
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
        let msg_id: i64 = queue
            .send(EMAIL_MSG_QUEUE, &msg)
            .await
            .expect("Failed to enqueue message");
        println!("Sent a message (id={}): {:?}", msg_id, msg);
    }

    Ok(())
}
