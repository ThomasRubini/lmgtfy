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
pub struct CommonMessage {
    pub contact: String,
    pub origin: String,
    pub body: String,
    pub timestamp: u64,
}

// FIXME: Move to a separate project

impl From<WhatsAppMessage> for CommonMessage {
    fn from(wa_msg: WhatsAppMessage) -> Self {
        CommonMessage {
            contact: wa_msg.sender,
            origin: "WhatsApp".to_string(),
            body: wa_msg.content,
            timestamp: wa_msg.timestamp,
        }
    }
}

impl From<EmailMessage> for CommonMessage {
    fn from(email_msg: EmailMessage) -> Self {
        CommonMessage {
            contact: email_msg.from,
            origin: "Email".to_string(),
            body: email_msg.content,
            timestamp: email_msg.timestamp,
        }
    }
}