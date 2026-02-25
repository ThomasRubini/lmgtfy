use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{
    LABELED_TICKETS_QUEUE,
    dto::LabeledTicket,
    queue::{kafka::KafkaQueueManager, Message, QueueManager},
};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

const STORAGE_DIR: &str = "./data/labeled_tickets";

struct TicketStorage {
    current_date: String,
    writer: Option<BufWriter<File>>,
}

impl TicketStorage {
    fn new() -> Result<Self> {
        // Créer le dossier de stockage s'il n'existe pas
        std::fs::create_dir_all(STORAGE_DIR)?;
        
        Ok(TicketStorage {
            current_date: String::new(),
            writer: None,
        })
    }

    fn get_file_path(date: &str) -> String {
        format!("{}/labeled_tickets_{}.jsonl", STORAGE_DIR, date)
    }

    fn ensure_writer(&mut self, date: &str) -> Result<()> {
        if self.current_date != date || self.writer.is_none() {
            // Fermer l'ancien writer si nécessaire
            if let Some(mut writer) = self.writer.take() {
                writer.flush()?;
            }

            // Ouvrir un nouveau fichier pour la nouvelle date
            let file_path = Self::get_file_path(date);
            println!("Opening file: {}", file_path);
            
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)?;
            
            self.writer = Some(BufWriter::new(file));
            self.current_date = date.to_string();
        }
        Ok(())
    }

    fn store_ticket(&mut self, ticket: LabeledTicket) -> Result<()> {
        let date_time = DateTime::from_timestamp(ticket.labeled_at as i64, 0)
            .unwrap_or_else(|| Utc::now());
        let date = date_time.format("%Y-%m-%d").to_string();
        
        self.ensure_writer(&date)?;

        let json_line = serde_json::to_string(&ticket)?;
        
        if let Some(ref mut writer) = self.writer {
            writeln!(writer, "{}", json_line)?;
            writer.flush()?;
        }

        println!("Stored labeled ticket {} with title: '{}'", ticket.id, ticket.title);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting Labeled Ticket Storage service...");
    println!("Storage directory: {}", STORAGE_DIR);

    let mut queue_mgr = KafkaQueueManager::new().await?;
    queue_mgr.create(LABELED_TICKETS_QUEUE).await?;

    println!("Listening for labeled tickets on '{}'...", LABELED_TICKETS_QUEUE);

    queue_mgr
        .register_read(LABELED_TICKETS_QUEUE, &async |msg: Message<LabeledTicket>| {
            let mut storage = TicketStorage::new()?;
            
            println!("Received labeled ticket (id={}): title='{}'", 
                     msg.msg_id, msg.message.title);
            
            if let Err(e) = storage.store_ticket(msg.message) {
                eprintln!("Failed to store labeled ticket {}: {}", msg.msg_id, e);
            } else {
                println!("Labeled ticket {} stored successfully", msg.msg_id);
            }
            
            Ok(())
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::dto::{CommonMessage, Origin};

    fn create_test_labeled_ticket() -> LabeledTicket {
        LabeledTicket {
            id: "test-123".to_string(),
            original_message: CommonMessage {
                contact: "test@example.com".to_string(),
                origin: Origin::Email,
                body: "Test message for storage".to_string(),
                timestamp: 1772000000,
                ticket_hint: Some("TEST-123".to_string()),
            },
            title: "Test Support Request".to_string(),
            tags: vec!["test".to_string(), "support".to_string()],
            description: "This is a test ticket for storage verification".to_string(),
            labeled_at: 1772000000,
        }
    }

    #[test]
    fn test_labeled_ticket_storage() -> Result<()> {
        let mut storage = TicketStorage::new()?;
        let ticket = create_test_labeled_ticket();
        
        storage.store_ticket(ticket)?;
        
        // Vérifier que le fichier a été créé
        let date_time = DateTime::from_timestamp(1772000000, 0).unwrap();
        let date = date_time.format("%Y-%m-%d").to_string();
        let file_path = TicketStorage::get_file_path(&date);
        assert!(Path::new(&file_path).exists());
        
        Ok(())
    }
}