use clap::Parser;
use common::{
    COMMON_MSG_QUEUE, EMAIL_MSG_QUEUE, PG_URL,
    dto::{CommonMessage, EmailMessage},
};
use pgmq::{Message, PGMQueueExt};
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
    println!("Connecting to Postgres");
    let pgmq = PGMQueueExt::new(PG_URL.to_string(), 1)
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    pgmq.create(EMAIL_MSG_QUEUE)
        .await
        .expect("Failed to create queue");

    loop {
        // Read a message
        let received_msg: Message<EmailMessage> =
            match pgmq.pop::<EmailMessage>(EMAIL_MSG_QUEUE).await.unwrap() {
                Some(msg) => msg,
                None => {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

        println!(
            "Received a message (id={}): {:?}",
            received_msg.msg_id, received_msg.message
        );

        match on_message_received(&pgmq, received_msg.message).await {
            Ok(_) => {
                pgmq.delete(EMAIL_MSG_QUEUE, received_msg.msg_id)
                    .await
                    .unwrap();
            }
            Err(err) => {
                eprintln!("Error processing message: {:?}", err);
            }
        }
    }
}

async fn on_message_received(pgmq: &PGMQueueExt, msg: EmailMessage) -> anyhow::Result<()> {
    let common_msg: CommonMessage = msg.into();
    pgmq.send(COMMON_MSG_QUEUE, &common_msg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to push message: {:?}", e))?;
    Ok(())
}
