use clap::Parser;
use common::{
    COMMON_MSG_QUEUE, EMAIL_MSG_QUEUE,
    dto::{CommonMessage, EmailMessage},
    queue::{Message, QueueManager, kafka::KafkaQueueManager},
};

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
    let mut queue_mgr = KafkaQueueManager::new().await.unwrap();

    // Create queues
    queue_mgr.create(EMAIL_MSG_QUEUE).await.expect(&format!(
        "Failed to create email topic '{}'",
        EMAIL_MSG_QUEUE
    ));
    queue_mgr.create(COMMON_MSG_QUEUE).await.expect(&format!(
        "Failed to create common topic '{}'",
        COMMON_MSG_QUEUE
    ));

    println!(
        "Listening for email messages on topic '{}'...",
        EMAIL_MSG_QUEUE
    );

    let queue_mgr_2 = KafkaQueueManager::new().await.unwrap();
    queue_mgr
        .register_read(EMAIL_MSG_QUEUE, &async |msg: Message<EmailMessage>| {
            // Transform to common message
            let common_msg: CommonMessage = msg.message.into();
            println!("Transformed to: {:?}", common_msg);

            // Send to common queue
            let forwarded_id = queue_mgr_2
                .send(COMMON_MSG_QUEUE, &common_msg)
                .await
                .expect(&format!(
                    "Failed to forward message {} to common queue",
                    msg.msg_id
                ));
            println!("Forwarded to common queue (id={})", forwarded_id);

            Ok(())
        })
        .await?;
    Ok(())
}
