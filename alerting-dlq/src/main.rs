use std::env;

use anyhow::Context;
use common::queue::QueueManager;
use common::queue::pgmq::PgMqQueueManager;
use reqwest::Client;
use serde_json::json;

fn get_webhook_url() -> anyhow::Result<String> {
    env::var("DISCORD_WEBHOOK_URL").context("DISCORD_WEBHOOK_URL unset")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let queue_mgr = PgMqQueueManager::new()
        .await
        .context("Failed to connect to postgres")?;

    // Check that we can get the URL:
    get_webhook_url()?;

    for queue in queue_mgr.list_queues().await? {
        if queue.ends_with("_dlq") {
            println!("Registering handler for queue: {}", queue);
            queue_mgr
                .register_read(&queue, &async |msg: common::queue::Message<
                    serde_json::Value,
                >| {
                    on_message(&queue, &msg.message).await
                })
                .await
                .context("Failed to register read handler")?;
        }
    }

    Ok(())
}

async fn on_message(queue: &str, msg: &serde_json::Value) -> anyhow::Result<()> {
    println!("Received message from {}: {:?}", queue, msg);
    let payload = json!({
        "content": format!("Received message from queue {}: {}", queue, msg)
    });
    let client = Client::new();

    let res = client
        .post(get_webhook_url()?)
        .json(&payload)
        .send()
        .await?;
    res.error_for_status()?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_on_message() {
        let queue = "test_queue_dlq";
        let msg = serde_json::Value::String("This is a test message".into());
        on_message(queue, &msg).await.unwrap();
    }
}
