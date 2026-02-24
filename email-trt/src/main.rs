use clap::Parser;
use common::{
    COMMON_MSG_QUEUE, EMAIL_MSG_QUEUE,
    dto::{CommonMessage, EmailMessage},
    queue::{QueueManager, pgmq::PgMqQueueManager},
};
use std::future;

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
    let queue_mgr = PgMqQueueManager::new().await.expect("Failed to connect to postgres");

    // Create queue
    queue_mgr.create(EMAIL_MSG_QUEUE).await?;
    queue_mgr.create(COMMON_MSG_QUEUE).await?;

    queue_mgr
        .register_read::<EmailMessage, _>(EMAIL_MSG_QUEUE, &async |msg| {
            let common_msg: CommonMessage = msg.message.into();
            queue_mgr
                .send(COMMON_MSG_QUEUE, &common_msg)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to push message: {:?}", e))?;
            Ok(())
        })
        .await?;

    future::pending::<()>().await;
    panic!("All tasks finished");
}
