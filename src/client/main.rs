#[path="../log.rs"]
mod log;
#[path="../event.rs"]
mod event;
#[path="../watcher.rs"]
mod watcher;
#[path="../indexer.rs"]
mod indexer;

use log::*;
use event::*;
use watcher::*;
use indexer::*;

use lazy_static::{lazy_static};
use serde::{Serialize, Deserialize};
use std::{env, thread, time, string::String};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use my_notify::*;
use rusoto_core::{Region};
use rusoto_sqs::{Sqs, SqsClient, ListQueueTagsRequest, CreateQueueRequest, DeleteMessageBatchRequest,
                 DeleteMessageBatchRequestEntry, ReceiveMessageRequest, ReceiveMessageResult};
use rusoto_s3::{S3, S3Client, HeadBucketRequest};
use std::sync::{Arc, Mutex};
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
struct ClientConfig {
    version: u8,
    client_id: String,
    sync_dir: String,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    s3_bucket: String,
    sqs_downstream: String,
    sqs_prefix: String
}

/* default configuration for client - '~/.config/sync-client' */
impl ::std::default::Default for ClientConfig {
    fn default() -> Self { Self { 
        version: 0,
        client_id: "".to_string(),
        sync_dir: "/opt/sync/".to_string(),
        aws_access_key_id: "".to_string(),
        aws_secret_access_key: "".to_string(),
        s3_bucket: "".to_string(),
        sqs_downstream: "".to_string(),
        sqs_prefix: "".to_string() } }
}

/* lazy_static is a useful way of declaring "runtime" statics */
lazy_static! {
    /* initialize a static cfg variable for use later */
    static ref CFG : ClientConfig = match confy::load("sync-client") {
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

    log(LogLevel::Info, "MySync-client starting up...");

    /* A "pause" lock for use by the indexer to halt the watcher */
    let pause: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let pause_watcher = pause.clone();

    /* channel for communication between indexer and watcher */
    let (tx, rx): (Sender<Event>, Receiver<Event>) = channel();

    let (notify_tx, notify_rx) = unbounded();
    let mut watcher = watcher(notify_tx.clone(), time::Duration::from_millis(5000)).unwrap();

    /* spawn the watcher thread */
    thread::spawn(move || {
        /* thread gets access to variables from outer scope */
        thread::sleep(time::Duration::from_secs(1));
        /* wait for the indexer to unpause */
        if let Err(e) = pause_watcher.lock() {
            log(LogLevel::Critical, &format!("Watcher was unable to obtain lock :: {}", e));
        }
        /* TODO: Debounced event timer cfg param */
        // let mut watcher = watcher(notify_tx, time::Duration::from_millis(5000)).unwrap();
        /* TODO: Sync folder cfg param */
        // watcher.watch(CFG.sync_dir.to_string(), RecursiveMode::Recursive).unwrap();

        /* main watcher event loop */
        /* TODO: Make channel errors in watcher recoverable (make a new one),
            indexer can function without watcher (just receives upstream updates) */
        loop {
            if let Err(e) = pause_watcher.lock() {
                log(LogLevel::Critical, &format!("Watcher was unable to obtain lock :: {}", e));
            }
            /* block until there is an event to be handled (nothing else to do) */
            match notify_rx.recv() {
                Ok(event) => {
                    /* received the event, now process it */
                    handle_inotify_event(event, &tx, &notify_rx);
                },
                /* something went horribly wrong, inotify possibly broken or dead channel, log it */
                Err(e) => {
                    log(LogLevel::Critical, &format!("Cannot receive message from inotify :: {}", e));
                },
            }
        }
    });
    /* end of watcher body */

    /* stop the watcher from doing anything while we set up the indexer */
    let pause_guard = pause.lock().unwrap();

    /* Need a kv "store" to link filename to signature,
        we start it up here... */
    // let metastore: HashMap<String, Signature> = HashMap::new();
    let mut full_sync = false;
    let mut metastore;
    match PickleDb::load_bin("test.db", PickleDbDumpPolicy::PeriodicDump(time::Duration::from_secs(5))) {
        Ok(db) => {
            metastore = db;
        },
        Err(_) => {
            log(LogLevel::Info, "Pre-existing db not found, creating new db...");
            metastore = PickleDb::new_bin("test.db", PickleDbDumpPolicy::AutoDump);
            full_sync = true;
        }
    }
    /* resync all the stuff on disk according to the metastore,
        This is where we would do so if we had the right data format */

    /* connect to s3 */
    let s3 = S3Client::new(Region::UsEast1);
    /* One global bucket serves all data (since it is stored as chunks, keyed by content hash) */
    /* TODO: Check if the sync bucket exists, we're hosed if not (panic!) */ 
    match s3.head_bucket(HeadBucketRequest { bucket: CFG.s3_bucket.clone() }).await {
        Ok(_) => log(LogLevel::Info, &format!("Successfully connected to S3 ({})", CFG.s3_bucket.clone())),
        Err(e) => {
            log(LogLevel::Critical, &format!("Cannot connect to S3 ({})", CFG.s3_bucket.clone()));
            panic!("{}", e)
        },
    }

    /* connect to sqs */
    let sqs = SqsClient::new(Region::UsEast1);
    /* verify that can connect to downstream queue (list tags is simplest way) */
    match sqs.list_queue_tags(ListQueueTagsRequest { queue_url: CFG.sqs_downstream.clone() }).await {
        Ok(_) => log(LogLevel::Info, "Successfully connected to SQS (sync-downstream.fifo)"),
        Err(e) => {
            log(LogLevel::Critical, "Cannot connect to SQS (sync-downstream.fifo)");
            panic!("{}", e);
        },
    }

    /* creates (if not exist) an upstream queue so we can -send- messages to the sync server,
        based on the client-id,
        (this would obviously be handled by an authentication server in production) */
    let cqr = CreateQueueRequest {
        queue_name: format!("sync-upstream-{}.fifo", CFG.client_id),
        attributes: Some(hashmap![String::from("FifoQueue") => String::from("true"), String::from("ContentBasedDeduplication") => String::from("true")]),
        ..Default::default()
    };
    match sqs.create_queue(cqr).await {
        Ok(_) => {
            log(LogLevel::Info, &format!("Successfully connected to SQS ({})", format!("sync-upstream-{}.fifo", CFG.client_id)));
        },
        Err(e) => {
            log(LogLevel::Critical, &format!("Cannot connect to SQS ({})", format!("sync-upstream-{}.fifo", CFG.client_id)));
            panic!("{}", e);
        },
    }

    let sqs_request = ReceiveMessageRequest {
        queue_url: format!("{}/sync-upstream-{}.fifo", CFG.sqs_prefix, CFG.client_id),
        max_number_of_messages: Some(10),
        ..Default::default()
    };

    /* if this is a new client, it needs to be fully sync'd */
    if full_sync {
        log(LogLevel::Info, "Performing full synchronization for new client...");
        if let None = fullsync(&mut metastore, &s3, &sqs, sqs_request.clone()).await {
            std::fs::remove_file("test.db");
            panic!("Cannot proceed without being synchronized...")
        }
        log(LogLevel::Info, "Done syncing...");
    } else {
        /* resync everything in the folder if there is a discrepancy */
        log(LogLevel::Info, "Checking files to resync between metastore and disk...");
        resync(&metastore, &s3).await;
        log(LogLevel::Info, "Done resyncing...");

    }
    
    for entry in WalkDir::new(&CFG.sync_dir) {
        // println!("entry: {}", &entry.unwrap().path().display());
        watcher.watch(entry.unwrap().path(), RecursiveMode::NonRecursive).unwrap();
    }
    /* can unlock the pause lock so that watcher may resume */
    std::mem::drop(pause_guard);
    log(LogLevel::Info, "MySync-client ready...");

    /* main indexer event loop */
    loop {
        // thread::sleep(time::Duration::from_secs(5));
        // println!("metastore: {:?}", metastore.get_all());
        /* try to receive messages from the sync server before handling local events */
        let event_msgs = receive_messages(&sqs, sqs_request.clone()).await;
        for event_msg in event_msgs {
            println!("event: {:?}", event_msg);
            handle_event_message(&event_msg, &mut metastore, &mut watcher, &s3).await;
        }

        /* fetch a file event, don't block if no events */
        match rx.try_recv() {
            Ok(msg) => {
                handle_watcher_event(msg, &s3, &sqs, &mut metastore, &mut watcher).await;                
            },
            Err(e) => {
                match e {
                    /* buffer is empty, do other stuff until opportunity to recv again */
                    TryRecvError::Empty => (),
                    /* disconnect error, have to bail */
                    TryRecvError::Disconnected => {
                        log(LogLevel::Critical, "Cannot receive message from watcher...");
                        panic!();
                    }
                }
            }
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
                ems.clear();
            }
        },
        /* This is a timeout from the recv request (dropped connection) */
        Err(e) => {
            log(LogLevel::Warning, &format!("Failed to receive messages :: {}", e));
        },
        /* This is when request is successful, but no messages recv'd */
        _ => ()
    };
    /* return a vec of eventmsgs (to send to handler), empty in case of errors */
    ems
}