use common::queue::QueueManager;
use common::{NEW_TICKET_QUEUE, dto::NewTicket, queue::pgmq::PgMqQueueManager};
use openrouter_rs::{
    OpenRouterClient,
    api::chat::*,
    types::{ResponseFormat, Role},
};
use pgmq::PgmqError;
use std::env;

const MODEL: &str = "openrouter/free";

fn get_api_key() -> String {
    env::var("OPENROUTER_API_KEY").expect("OPENROUTER_API_KEY must be set")
}

#[tokio::main]
async fn main() -> Result<(), PgmqError> {
    let queue_mgr = PgMqQueueManager::new()
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    queue_mgr
        .create(NEW_TICKET_QUEUE)
        .await
        .expect("Failed to create queue");

    // Init LLM
    let client = OpenRouterClient::builder()
        .api_key(get_api_key())
        .build()
        .expect("Failed to create OpenRouter client");

    queue_mgr
        .register_read(NEW_TICKET_QUEUE, &|msg| {
            on_message(&client, msg.message)
        })
        .await
        .expect("Failed to register read handler");

    Ok(())
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

async fn on_message(client: &OpenRouterClient, msg: NewTicket) -> anyhow::Result<()> {
    println!("Received a message: {:?}", msg);

    labelize_message(client, &msg).await?;
    // TODO post to postgres

    Ok(())
}

async fn labelize_message(client: &OpenRouterClient, msg: &NewTicket) -> anyhow::Result<FormattedTicket> {
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
    Ok(formatted_ticket)
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
            .api_key(get_api_key())
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
}
