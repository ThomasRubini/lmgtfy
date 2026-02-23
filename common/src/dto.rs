use serde::{Deserialize, Serialize};



#[derive(Serialize, Debug, Deserialize)]
pub struct WhatsAppMessage {
    pub sender: String,
    pub content: String,
    pub timestamp: u64,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct EmailMessage {
    pub from: String,
    pub to: String,
    pub content: String,
    pub timestamp: u64,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct Message {
    pub contact: String,
    pub origin: String,
    pub body: String,
    pub timestamp: u64,
}

// FIXME: Move to a separate project

impl From<WhatsAppMessage> for Message {
    fn from(wa_msg: WhatsAppMessage) -> Self {
        Message {
            contact: wa_msg.sender,
            origin: "WhatsApp".to_string(),
            body: wa_msg.content,
            timestamp: wa_msg.timestamp,
        }
    }
}

impl From<EmailMessage> for Message {
    fn from(wa_msg: EmailMessage) -> Self {
        Message {
            contact: wa_msg.from,
            origin: "Email".to_string(),
            body: wa_msg.content,
            timestamp: wa_msg.timestamp,
        }
    }
}