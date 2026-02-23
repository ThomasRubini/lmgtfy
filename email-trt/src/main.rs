use clap::Parser;
use common::{EMAIL_MSG_QUEUE, PG_URL, VISIBILITY_TIMEOUT_SECONDS, dto::EmailMessage};
use pgmq::{Message, PGMQueueExt, PgmqError};
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
async fn main() -> Result<(), PgmqError> {
    println!("Connecting to Postgres");
    let queue = PGMQueueExt::new(PG_URL.to_string(), 1)
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    queue
        .create(EMAIL_MSG_QUEUE)
        .await
        .expect("Failed to create queue");

    loop {
        // Read a message
        let received_msg: Message<EmailMessage> = match queue
            .read::<EmailMessage>(EMAIL_MSG_QUEUE, VISIBILITY_TIMEOUT_SECONDS)
            .await
            .unwrap()
        {
            Some(msg) => msg,
            None => {
                println!("No messages in the queue, retrying...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        println!("Received a message: {:?}", received_msg);
    }
}
