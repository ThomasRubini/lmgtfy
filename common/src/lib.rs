use pgmq::PGMQueueExt;

pub mod dto;

pub const VISIBILITY_TIMEOUT_SECONDS: i32 = 30;
pub const PG_URL: &str = "postgres://postgres:postgres@0.0.0.0:5432";
pub const WHATSAPP_MSG_QUEUE: &str = "whatsapp_messages";
pub const EMAIL_MSG_QUEUE: &str = "email_messages";
pub const COMMON_MSG_QUEUE: &str = "common_messages";
