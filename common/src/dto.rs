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
pub enum Origin {
    WhatsApp,
    Email,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct CommonMessage {
    pub contact: String,
    pub origin: Origin,
    pub body: String,
    pub timestamp: u64,
    pub ticket_hint: Option<String>,
}

// FIXME: Move to a separate project

impl From<WhatsAppMessage> for CommonMessage {
    fn from(wa_msg: WhatsAppMessage) -> Self {
        CommonMessage {
            contact: wa_msg.sender,
            origin: Origin::WhatsApp,
            body: wa_msg.content,
            timestamp: wa_msg.timestamp,
            ticket_hint: None,
        }
    }
}

impl From<EmailMessage> for CommonMessage {
    fn from(email_msg: EmailMessage) -> Self {
        let ticket_hint = email_msg
            .to
            .split('+')
            .nth(1)
            .and_then(|s| s.split('@').next())
            .map(|s| s.to_string());
        CommonMessage {
            contact: email_msg.from,
            origin: Origin::Email,
            body: email_msg.content,
            timestamp: email_msg.timestamp,
            ticket_hint: ticket_hint,
        }
    }
}
