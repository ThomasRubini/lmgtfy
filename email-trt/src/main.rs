use clap::Parser;
use common::{
    COMMON_MSG_QUEUE, EMAIL_MSG_QUEUE,
    dto::{CommonMessage, EmailMessage},
    queue::{Message, QueueManager, pgmq::PgMqQueueManager},
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
    println!("Connecting to Postgres");
    let queue_mgr = PgMqQueueManager::new().await;

    // Create queue
    queue_mgr.create(EMAIL_MSG_QUEUE).await?;
    queue_mgr.create(COMMON_MSG_QUEUE).await?;

    loop {
        // Read a message
        let received_msg: Message<EmailMessage> = match queue_mgr
            .read::<EmailMessage>(EMAIL_MSG_QUEUE)
            .await
            .unwrap()
        {
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

        match on_message_received(&queue_mgr, received_msg.message).await {
            Ok(_) => {
                queue_mgr
                    .delete(EMAIL_MSG_QUEUE, received_msg.msg_id)
                    .await
                    .unwrap();
            }
            Err(err) => {
                eprintln!("Error processing message: {:?}", err);
            }
        }
    }
}

async fn on_message_received(pgmq: &impl QueueManager, msg: EmailMessage) -> anyhow::Result<()> {
    let common_msg: CommonMessage = msg.into();
    pgmq.send(COMMON_MSG_QUEUE, &common_msg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to push message: {:?}", e))?;
    Ok(())
}
