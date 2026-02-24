use anyhow::Context;
use common::{NEW_TICKET_QUEUE, PG_URL, VISIBILITY_TIMEOUT_SECONDS, dto::NewTicket};
use openrouter_rs::{
    OpenRouterClient,
    api::chat::*,
    types::{ProviderPreferences, ResponseFormat, Role},
};
use pgmq::{Message as MQMessage, PGMQueueExt, PgmqError};
use std::{env, time::Duration};
use tokio::time::sleep;

const MODEL: &str = "openrouter/free";
const READ_QUEUE: &str = NEW_TICKET_QUEUE;
const API_KEY: &str = env!("OPENROUTER_API_KEY");

#[tokio::main]
async fn main() -> Result<(), PgmqError> {
    println!("Connecting to Postgres, queue: {}", READ_QUEUE);
    let queue = PGMQueueExt::new(PG_URL.to_string(), 1)
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    queue
        .create(READ_QUEUE)
        .await
        .expect("Failed to create queue");

    // Init LLM
    let client = OpenRouterClient::builder()
        .api_key(API_KEY)
        .build()
        .expect("Failed to create OpenRouter client");

    loop {
        // Read a message
        let received_msg: MQMessage<NewTicket> = match queue
            .read::<NewTicket>(READ_QUEUE, VISIBILITY_TIMEOUT_SECONDS)
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
        on_message(&client, received_msg.message).await;
    }
}

async fn on_message(client: &OpenRouterClient, msg: NewTicket) {
    println!("Received a message: {:?}", msg);

    match labelize_message(client, &msg).await {
        Ok(_) => {
            println!("Message processed successfully: {:?}", msg);
            // Acknowledge the message
            // queue.ack(received_msg.id).await.unwrap();
        }
        Err(e) => {
            eprintln!("Error processing message {:?}: {:?}", msg, e);
            // TODO: To DLQ
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct FormattedTicket {
    id: String,
    title: String,
    tags: Vec<String>,
    description: String,
}


#[derive(Debug, serde::Deserialize)]
struct LLMResponse {
    title: String,
    tags: Vec<String>,
    description: String,
}

async fn labelize_message(client: &OpenRouterClient, msg: &NewTicket) -> anyhow::Result<()> {
    println!("Processing message: {:?}", msg);

    // send to llm, expect a title, tags and description
    let input_json = serde_json::to_string(&msg)?;

    let user_prompt = format!(
        "Given this ticket, :\n{}\nGenerate its metadata. Output ONLY the JSON, without any additional text.",
        input_json
    );

    let format = ResponseFormat::json_schema(
        "labelled_ticket",
        true,
        serde_json::json!({
          "type": "object",
          "properties": {
            "title": {
              "type": "string",
              "description": "A concise title for the ticket, summarizing the main issue or request.",
            },
            "tags": {
              "type": "array",
              "items": {
                "type": "string"
              },
              "description": "Tags for the ticket: a list of relevant keywords or categories that apply to the ticket, such as 'database', 'outage', 'production', etc.",
            },
            "description": {
              "type": "string",
              "description": "A TL;DR of the ticket, summarizing the key details and context in a few sentences.",
            },
          },
          "additionalProperties": false,
          "required": ["title", "tags", "description"]
        }),
    );

    // Send request
    let request = ChatCompletionRequest::builder()
        .model(MODEL)
        .messages(vec![Message::new(Role::User, user_prompt)])
        .response_format(format)
        .build()?;

    println!("Sending request to LLM...");

    // Extract raw text
    let response = client.send_chat_completion(&request).await?;
    let content = response.choices[0]
        .content()
        .expect("LLM Content should be present")
        .to_string();
    println!("Response: {:#?}", content);

    let parsed = serde_json::from_str::<LLMResponse>(&content)?;
    println!("Parsed response: {:?}", parsed);

    // Deserialize into struct
    let formatted_ticket = FormattedTicket {
        id: msg.id.clone(),
        title: parsed.title,
        tags: parsed.tags,
        description: parsed.description,
        
    };
    eprintln!("Formatted ticket: {:#?}", formatted_ticket);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::dto::CommonMessage;

    // Helper to create a fake NewTicket
    fn fake_new_ticket() -> NewTicket {
        NewTicket {
            id: "384594739".to_string(),
            init_message: CommonMessage {
                contact: "jack.hammer@mycom.com".to_string(),
                origin: common::dto::Origin::WhatsApp,
                body: "Hello, \n Our production database is down across all regions. Can you help us solve this issue ASAP?".to_string(),
                timestamp: 0,
                ticket_hint: None,
            },
        }
    }

    #[tokio::test]
    async fn test_labelize_message_with_fake_ticket() {
        let client = OpenRouterClient::builder()
            .api_key(API_KEY)
            .build()
            .expect("Failed to create OpenRouter client");

        let ticket = fake_new_ticket();

        let result = labelize_message(&client, &ticket).await;
        assert!(
            result.is_ok(),
            "LLM processing should succeed with a fake ticket: \n {:?}",
            result.err()
        );
    }
    /*
    #[tokio::test]
    async fn test_on_message_with_fake_ticket() {
        let client = OpenRouterClient::builder()
            .api_key("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")
            .build()
            .expect("Failed to create OpenRouter client");

        let ticket = fake_new_ticket();

        // This just checks that the function runs without panicking
        on_message(&client, ticket).await;
    }
    */
}
