#[path="../log.rs"]
mod log;
#[path="../event.rs"]
mod event;

use log::*;
use event::*;

use lazy_static::{lazy_static};
use serde::{Serialize, Deserialize};
use std::{env, thread, time, fs, string::String, collections::HashMap, collections::HashSet};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use my_notify::*;
use my_rustsync::*;
use rusoto_core::{Region};
use rusoto_sqs::{Sqs, SqsClient, ListQueuesRequest, CreateQueueRequest, DeleteMessageBatchRequest,
                 DeleteMessageBatchRequestEntry, ReceiveMessageRequest, SendMessageRequest, ReceiveMessageResult, Message};
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, ListTablesInput, PutItemInput, AttributeValue, DeleteItemInput};
use std::io::prelude::*;
use tokio::io;
use std::sync::{Arc, RwLock, Mutex};
use pickledb::{PickleDb, PickleDbDumpPolicy};
use walkdir::WalkDir;
use crossbeam::crossbeam_channel::unbounded;

/* 10MB, should never be changed once files are chunked on S3 */
const CHUNK_SIZE : usize = 10485760;

/* macro for inline hashmap definition  & initialization */
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

/* struct for client configuration */
#[derive(Debug, Serialize, Deserialize)]
struct ServerConfig {
    version: u8,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    s3_bucket: String,
    sqs_downstream: String
}

/* default configuration for client - '~/.config/sync-client' */
impl ::std::default::Default for ServerConfig {
    fn default() -> Self { Self { 
        version: 0,
        aws_access_key_id: "".to_string(),
        aws_secret_access_key: "".to_string(),
        s3_bucket: "".to_string(),
        sqs_downstream: "".to_string() } }
}

/* lazy_static is a useful way of declaring "runtime" statics */
lazy_static! {
    /* initialize a static cfg variable for use later */
    static ref CFG : ServerConfig = match confy::load("sync-server") {
        Ok(c) => c,
        Err(e) => panic!("Configuration file could not be loaded! - {}", e)
    };
}

/* Indexer thread launches watcher thread */
#[tokio::main]
async fn main() {
    /* have to deref a lazy_static var, it's given a one-off type by lazy_static */
    println!("{:#?}", *CFG);

    /* initialize the ENV creds */
    /* ONLY for testing, access keys should be in an env file, not on github */
    env::set_var("AWS_ACCESS_KEY_ID", &CFG.aws_access_key_id);
    env::set_var("AWS_SECRET_ACCESS_KEY", &CFG.aws_secret_access_key);

    log(LogLevel::Info, "MySync-Server Starting up...");

    /* for iterating over to broadcast */
    let mut clientstore: Vec<(String, String)> = Vec::new();

    /* connect to sqs (downstream is what we receive on for the server) */
    let sqs = SqsClient::new(Region::UsEast1);
    /* verify that can connect to downstream queue (list tags is simplest way) */
    match sqs.list_queues(ListQueuesRequest { ..Default::default() }).await {
        Ok(res) =>{
            log(LogLevel::Info, "Successfully connected to SQS (sync-downstream.fifo)");
            match res.queue_urls {
                Some(qs) => {
                    // println!("qs: {:?}", qs);
                    for q in qs {
                        /* extremely fragile way of accomplishing this... ehhhh */
                        if q != CFG.sqs_downstream {
                            clientstore.push((q[(q.rfind("sync-upstream-").unwrap() + 14)..(q.len() - 5)].to_string(), q))
                        }
                    }
                    if clientstore.is_empty() {
                        return log(LogLevel::Critical, "No queues to broadcast to... exitting.");
                    }
                },
                None => {
                    return log(LogLevel::Critical, "No queues to broadcast to... exitting.");
                }
            }
            log(LogLevel::Debug, "Collected list of queues to broadcast")
        },
        Err(e) => {
            return log(LogLevel::Critical, &format!("Cannot connect to SQS (sync-downstream.fifo) :: {}", e));
        },
    }
    println!("cs: {:?}", clientstore);

    /* request struct for getting downstream messages */
    let sqs_request = ReceiveMessageRequest {
        queue_url: String::from(&CFG.sqs_downstream),
        max_number_of_messages: Some(10),
        ..Default::default()
    };

    /* connect to dynamodb */
    let db = DynamoDbClient::new(Region::UsEast1);
    match db.list_tables(ListTablesInput { ..Default::default() }).await {
        Ok(res) => {
            println!("res: {:?}", res);
        },
        Err(e) => {
            return log(LogLevel::Critical, &format!("Cannot connect to DynamoDB :: {}", e));
        }
    }


    // let block_buf = vec![0; CHUNK_SIZE];
    /* main indexer event loop */
    loop {
        thread::sleep(time::Duration::from_secs(5));
        // println!("metastore: {:?}", metastore.get_all());
        /* try to receive messages from the sync server before handling local events */
        let event_msgs = receive_messages(&sqs, sqs_request.clone()).await;
        for event_msg in event_msgs {
            println!("em: {:?}", event_msg);
            handle_event_message(&event_msg, &clientstore, &sqs, &db).await;
        }
    }
}

async fn receive_messages(sqs: &SqsClient, req: ReceiveMessageRequest) -> Vec<EventMessage> {
    let mut ems = Vec::new();
    let mut receipts = Vec::new();
    let qurl: String = req.queue_url.clone();

    let result = tokio::time::timeout(time::Duration::from_secs(30), sqs.receive_message(req)).await;
    match result {
        /* love rust pattern matching */
        Ok(Ok(ReceiveMessageResult { messages: Some(msgs) })) => {
            /* messages were recv'd, now you should process them! */
            for m in msgs {
                let em: EventMessage = serde_json::from_str(&m.body.unwrap())
                                        .unwrap_or(EventMessage { c: None, e: None, d: None, });
                ems.push(em);
                /* if a message doesn't have a receipt handle, that is horrible and will break things
                    (at that point, AWS is probably down or bugged) */
                receipts.push(m.receipt_handle.unwrap());
            }
            /* now batch delete the messages that were recv'd, block until done */
            let mut r_id = -1;
            let req = DeleteMessageBatchRequest {
                /* ah yes, rust's iter-map functionality... it's great! */
                entries: receipts.into_iter().map(|r| {
                    r_id += 1;
                    DeleteMessageBatchRequestEntry { id: r_id.to_string(), receipt_handle: r}
                }).collect(),
                queue_url: qurl,
            };
            if let Err(e) = sqs.delete_message_batch(req).await {
                /* couldn't delete messages, drop the event msgs!
                    (otherwise we would receive them again) */
                println!("{:?}", e);
                ems.clear();
            }
        },
        /* This handles the case of an http dispatch error (occuring usually after a reconnect)*/
        Ok(Err(e)) => {
            println!("Ok(e): {:?}", e);
        },
        /* This is a timeout from the recv request (dropped connection) */
        Err(e) => {
            println!("e: {:?}", e);
        },
        /* This is when request is successful, but no messages recv'd */
        _ => ()
    };
    /* return a vec of eventmsgs (to send to handler), empty in case of errors */
    ems
}

async fn handle_event_message(em: &EventMessage, cs: &Vec<(String, String)>, sqs: &SqsClient, db: &DynamoDbClient) {
    /* the server's responsibility is to basically forward client messages to eachother,
        and to a greater extent, do some more complex synchronization behaviors, but I most likely
        won't be able to get those done, oh well... */
    /* have to send a message to each client that isn't the originator */
    let (eme, emc) = (em.e.as_ref().unwrap(), em.c.as_ref().unwrap());
    /* persist the event's change in the database */
    handle_event(eme, &em.d, db).await;

    for (client_id, client_q) in cs {
        if *client_id != *emc {
            let request = SendMessageRequest {
                message_body: serde_json::to_string(&em).unwrap(),
                message_group_id: Some(String::from("sync")),
                queue_url: String::from(client_q),
                ..Default::default()
            };
            match sqs.send_message(request).await {
                Ok(res) => log(LogLevel::Debug, &format!("Event ({:?}) from client ({}) succesfully sent upstream to client ({})",
                                                        eme, emc, client_id)),
                Err(e) => log(LogLevel::Critical, &format!("Event ({:?}) from client ({}) failed to send upstream to client ({})",
                                                        eme, emc, client_id))
            }
        }
    }
}

async fn handle_event(e: &Event, d: &Option<MyDelta>, db: &DynamoDbClient) {
    match e {
        /* put the entry (f) with chunk ordering (f) in the database */
        Event::Write(f) => {
            if let Some(delta) = d {
                let chunks: Vec<String> = delta.blocks.clone().into_iter().map(|x| x.to_string()).collect();
                let mut req = PutItemInput {
                    table_name: "file".to_string(),
                    item: hashmap![
                        "path".to_string() => AttributeValue { s: Some(f.to_string()), ..Default::default()},
                        "chunks".to_string() => AttributeValue { ns: Some(chunks), ..Default::default()}
                    ],
                    ..Default::default()
                };
                db.put_item(req).await;
            }
        },
        /* have to replace the old entry (f) with the new entry (t) in the databse */
        Event::Rename(f, t) => {
            /* However, this must be done by creating t and then remove f, and
                technically, this should also be a "transaction" in a traditional sense */
            let mut req = DeleteItemInput {
                table_name: "file".to_string(),
                key: hashmap![
                    "path".to_string() => AttributeValue { s: Some(f.to_string()), ..Default::default()}
                ],
                return_values: Some("ALL_OLD".to_string()),
                ..Default::default()
            };
            match db.delete_item(req).await {
                Ok(item) => {
                    /* get the chunks and insert them for the entry (t) */
                    if let Some(attr) = item.attributes {
                        if let Some(attr_val) = attr.get("chunks") {
                            let mut req = PutItemInput {
                                table_name: "file".to_string(),
                                item: hashmap![
                                    "path".to_string() => AttributeValue { s: Some(t.to_string()), ..Default::default()},
                                    "chunks".to_string() => AttributeValue { ns: attr_val.ns.clone(), ..Default::default()}
                                ],
                                ..Default::default()
                            };
                            db.put_item(req).await;
                        }
                    } else {

                    }
                },
                Err(e) => {
                    /* the request failed, log it */
                    log(LogLevel::Critical, &format!("Could not update database for Event::Rename({} -> {}) :: {}", f, t, e));
                }
            }
        },
        /* get rid of the entry (f) in the database */
        Event::Remove(f) => {
            let mut req = DeleteItemInput {
                table_name: "file".to_string(),
                key: hashmap![
                    "path".to_string() => AttributeValue { s: Some(f.to_string()), ..Default::default()}
                ],
                return_values: Some("ALL_OLD".to_string()),
                ..Default::default()
            };
            println!("res: {:?}", db.delete_item(req).await);
        }
    }    
}