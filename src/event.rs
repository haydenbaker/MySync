use serde::{Serialize, Deserialize};
use my_rustsync::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum Event {
    Write(String),
    Rename(String, String),
    Remove(String)
}

/* struct to serialize to send via sqs */
#[derive(Debug, Serialize, Deserialize)]
pub struct EventMessage {
    pub c: Option<String>,
    pub e: Option<Event>,
    pub d: Option<MyDelta>
}