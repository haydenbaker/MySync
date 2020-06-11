use pickledb::{PickleDb};
use rusoto_s3::{S3, S3Client, PutObjectRequest, GetObjectRequest, HeadObjectRequest};
use rusoto_sqs::{Sqs, SqsClient, SendMessageRequest, ReceiveMessageRequest};
use my_rustsync::*;
use std::{path::Path, fs, fs::File, string::String, collections::HashMap};
use tokio::io;
use my_notify::{Result, inotify::INotifyWatcher};
use std::io::prelude::*;
use blake2_rfc;
use my_notify::{Watcher, RecursiveMode};

use crate::event::*;
use crate::log::*;
use crate::CHUNK_SIZE;
use crate::CFG;
use crate::receive_messages;
use my_rustsync::BLAKE2_SIZE;

pub async fn fullsync(metastore: &mut PickleDb, s3: &S3Client, sqs: &SqsClient, req: ReceiveMessageRequest) -> Option<()> {
    /* client will request a sync from the sync server, and sync until
        it has received confirmation that the client is fully sync'd */
    /* client must send sync event message first */
    let request = SendMessageRequest {
        message_body: serde_json::to_string(&EventMessage {
            c: Some(String::from(&CFG.client_id)),
            e: Some(Event::Synchronize(true)),
            d: None,
        }).unwrap(),                                   
        message_group_id: Some(CFG.client_id.to_string()),
        queue_url: CFG.sqs_downstream.clone(),
        ..Default::default()
    };
    match sqs.send_message(request).await {
        Ok(res) => log(LogLevel::Debug, &format!("Synchronization request ({}) sent, awaiting response...", res.message_id.unwrap())),
        Err(e) => {
            log(LogLevel::Critical, &format!("Could not send synchronization request, cannot proceed... :: {}", e));
            return None
        }
    }

    'outer: loop {
        let event_msgs = receive_messages(&sqs, req.clone()).await;
        for event_msg in event_msgs {
            match &event_msg.e.unwrap() {
                Event::Write(filename) => {
                    /* sync the file! */
                    let f = format!("{}{}", CFG.sync_dir, filename);
                    match &event_msg.d {
                        Some(d) => {
                            apply_file_write(&f, &d, metastore, s3).await;
                        },
                        None => {
                            log(LogLevel::Critical, &format!("Received file ({}) create, but no chunks!", f));
                        }
                    }
                },
                Event::Synchronize(_) => {
                    /* synchronization is done! */
                    break 'outer;
                },
                _ => {
                    log(LogLevel::Warning, &format!("Received invalid synchronization event"));
                },
            }
        }
    }
    Some(())
}

pub async fn resync(metastore: &PickleDb, s3: &S3Client) -> Result<()> {
    /* iterate over every entry in the metastore, new files on disk that were
        created won't be "resync'd", simply because they were never created in the eyes
        of the client (and they'll be sync'd upon next time they are modified) */
    for path in metastore.get_all() {
        resync_file(&path, metastore, s3).await;
    }
    Ok(())
}

pub async fn resync_file(path: &str, metastore: &PickleDb, s3: &S3Client) -> Result<()> {
    let mut block_map: HashMap<u32, Vec<u8>> = HashMap::new();
    match metastore.get::<Signature>(path) {
        Some(sig) => {
            /* sig is the last known signature for the file */
            match fs::read(path) {
                Ok(source) => {
                    /* file exists, must compare signature, and restore if differ */
                    let mut mod_sig = signature(&source[..], vec![0; CHUNK_SIZE]).unwrap();
                    if sig != mod_sig {
                        /* sig not match, handle discrepancies by iterating through entire ms-sig,
                            and cross-referencing the chunks */
                        log(LogLevel::Warning, &format!("Resyncing from disk for file ({})", path));
                        if let Ok(mut file) = fs::OpenOptions::new().write(true).create(true).open(path) {
                            for chunk_id in sig.chunks.keys() {
                                for offset in sig.chunks.get(chunk_id).unwrap().values().next().unwrap() {
                                    file.seek(std::io::SeekFrom::Start(*offset as u64));
                                    /* get the chunk offsets from source if it has them, otherwise s3 */
                                    match mod_sig.chunks.get(chunk_id) {
                                        Some(b2b) => {
                                            /* chunk is present in modified file */
                                            let offset_u = b2b.values().next().unwrap().first().unwrap();
                                            write_chunk_from_source(path, *chunk_id, *offset_u, *offset, &mut file, &mut mod_sig, &source, false);
                                        },
                                        None => {
                                            /* get it from s3 */
                                            write_chunk_from_s3(path, *chunk_id, *offset, &mut file, &mut mod_sig, &mut block_map, &s3, false).await;
                                        }
                                    }
                                }
                            }
                            log(LogLevel::Debug, &format!("File ({}) has been resync'd with the metastore", path));
                        } else {
                            log(LogLevel::Critical, &format!("Could not open file ({}) for writing to resync", path));
                        }
                    }
                    Ok(())
                },
                Err(e) => {
                    /* file isn't on disk, have to restore it via s3 */
                    log(LogLevel::Warning, &format!("Unable to restore from disk for file ({}), must resync from storage :: {}", path, e));
                    if let Ok(mut file) = fs::OpenOptions::new().write(true).create(true).open(path) {
                        for chunk_id in sig.chunks.keys() {
                            for offset in sig.chunks.get(chunk_id).unwrap().values().next().unwrap() {
                                file.seek(std::io::SeekFrom::Start(*offset as u64));
                                write_chunk_from_s3(path, *chunk_id, *offset, &mut file, &mut Signature { window: 0, chunks: HashMap::new() },
                                     &mut block_map, &s3, false).await;
                            }
                        }
                        log(LogLevel::Debug, &format!("File ({}) has been resync'd with the metastore", path));
                    } else {
                        log(LogLevel::Critical, &format!("Could not open file ({}) for writing to resync", path));
                    }
                    Ok(())
                }
            }
        },
        None => {
            /* no signature found for the file (so it's a new file), ignore it */
            Ok(())
        }
    }
}


pub async fn handle_watcher_event(msg: Event, s3: &S3Client, sqs: &SqsClient, metastore: &mut PickleDb, watcher: &mut INotifyWatcher) {
    match msg {
        /* a file or dir creation or modification */
        Event::Write(f) => {
            /* handle if f is dir here */
            match fs::metadata(&f) {
                Ok(md) => {
                    if md.is_dir() {
                        /* f is a dir, handle dir create */
                        handle_dir_create(&f, &s3, &sqs, metastore, watcher).await;
                    } else {
                        /* f is a file, handle file create */
                        handle_file_create(&f, &s3, &sqs, metastore).await;
                    }
                    watcher.watch(&f, RecursiveMode::NonRecursive);
                },
                Err(e) => {
                    /* unable to get the f's metadata, log it */
                    log(LogLevel::Critical, &format!("Could not read source file ({}) metadata! :: {}", f, e))
                }
            }
        },
        /* a file or dir rename (AKA move) */
        Event::Rename(f, t) => {
            /* filenames for sqs message later */
            let (_, f_filename) = f.split_at(CFG.sync_dir.len());
            let (_, t_filename) = t.split_at(CFG.sync_dir.len());
            match fs::metadata(&t) {
                Ok(md) => {
                    if md.is_dir() {
                        /* if a dir is renamed, need to change all of the signature in metastore,
                            but only need to send message for the dir rename */
                        apply_dir_rename(&t, &f, metastore, watcher);
                    } else {
                        apply_file_rename(&f, &t, metastore);
                    }
                    watcher.watch(&t, RecursiveMode::NonRecursive);
                },
                Err(e) => {
                    log(LogLevel::Critical, &format!("Could not read source file ({}) metadata! :: {}", f, e))
                }
            }
            let request = SendMessageRequest {
                message_body: serde_json::to_string(&EventMessage {
                    c: Some(String::from(&CFG.client_id)),
                    e: Some(Event::Rename(f_filename.to_string(), t_filename.to_string())),
                    d: None,
                }).unwrap(),                                   
                message_group_id: Some(CFG.client_id.to_string()),
                queue_url: CFG.sqs_downstream.clone(),
                ..Default::default()
            };
            match sqs.send_message(request).await {
                Ok(res) => log(LogLevel::Debug, &format!("Event(Rename::{} -> {}) Message (id: {}) succesfully sent downstream",
                                                        f_filename, t_filename, res.message_id.unwrap())),
                Err(e) => log(LogLevel::Critical, &format!("Event(Remove::{} -> {}) Message failed to send :: {}",
                                                        f_filename, t_filename, e))
            }
            watcher.watch(&t, RecursiveMode::NonRecursive);
        },
        /* a file or dir remove */
        Event::Remove(f) => {
            let (_, f_filename) = f.split_at(CFG.sync_dir.len());
            /* Send a remove message to sync server */
            let request = SendMessageRequest {
                message_body: serde_json::to_string(&EventMessage {
                    c: Some(String::from(&CFG.client_id)),
                    e: Some(Event::Remove(f_filename.to_string())),
                    d: None,
                }).unwrap(),                                   
                message_group_id: Some(CFG.client_id.to_string()),
                queue_url: CFG.sqs_downstream.clone(),
                ..Default::default()
            };
            match sqs.send_message(request).await {
                Ok(res) => log(LogLevel::Debug, &format!("Event(Remove::{}) Message (id: {}) succesfully sent downstream",
                                                        f_filename, res.message_id.unwrap())),
                Err(e) => log(LogLevel::Critical, &format!("Event(Remove::{}) Message failed to send :: {}",
                                                        f_filename, e))
            }
            /* remove f from the metastore, or do nothing if not there */
            metastore.rem(&f).unwrap();
        },
        _ => {
            /* this is where we could re-sync a client if the sync server determined it was inconsistent */
        },
    }
}

pub async fn handle_dir_create(path: &str, s3: &S3Client, sqs: &SqsClient, metastore: &mut PickleDb, watcher: &mut INotifyWatcher) {
    /* if a dir create event, there could be files in the dir that must be propogated */
    match fs::read_dir(path) {
        Ok(ents) => {
            for opt_ent in ents {
                /* unwrap the entry since read dir returns them as results */
                if let Ok(ent) = opt_ent {
                    let entry_path = ent.path();
                    let path_str = entry_path.clone().into_os_string().into_string().unwrap();
                    if entry_path.is_dir() {
                        /* recurse on a dir */
                        handle_dir_create(&path_str, s3, sqs, metastore, watcher);
                    } else {
                        /* ent is a file, so we must handle file create */
                        handle_file_create(&path_str, s3, sqs, metastore);
                    }
                    watcher.watch(&path_str, RecursiveMode::NonRecursive);
                } 
            }
        },
        Err(e) => {
            log(LogLevel::Critical, &format!("Could not read dir ({}) :: {}", path, e));
        }
    }
}

pub async fn validate_chunk(chunk_id: u32, s3: &S3Client) -> bool {
    let request = HeadObjectRequest {
        bucket: String::from(&CFG.s3_bucket),
        key: chunk_id.to_string(),
        ..Default::default()
    };
    if let Err(_) = s3.head_object(request).await {
        return false
    }
    return true;
}

pub async fn handle_file_create(path: &str, s3: &S3Client, sqs: &SqsClient, metastore: &mut PickleDb) {
    /* filename for sqs message later */
    let (_, filename) = path.split_at(CFG.sync_dir.len());
    let source = fs::read(path);
    
    /* if can't open file (maybe it got deleted), log it */
    match source {
        Ok(source) => {
            /* compare signature against whatever is in metastore */
            let source_sig = signature(&source[..], vec![0; CHUNK_SIZE]).unwrap();
            /* must figure out what new chunks to send to S3 */
            match metastore.get::<Signature>(path) {
                Some(sig) => {
                    if source_sig == sig {
                        log(LogLevel::Debug, &format!("Event(Create::{}) contents identical", path));
                        return;
                    }
                    /* metastore has a record already, must compare, upload if not present */
                    for (k, v) in source_sig.chunks.iter().filter(|&(k, _)| !sig.chunks.contains_key(k)) {
                        /* check if chunks exists on s3 already */
                        if validate_chunk(*k, s3).await {
                            continue;
                        }
                        /* chunk not present, so upload the chunk data using offset (in 'v') from signature */
                        /* TODO: CHANGE HERE FOR OFFLINE EDITS */
                        let offset : usize = *v.values().next().unwrap().first().unwrap();
                        let offset_end : usize = if offset + CHUNK_SIZE <= source.len() { offset + CHUNK_SIZE } else { source.len() };
                        /* Really should compress chunk before sending to S3 */
                        match s3.put_object(PutObjectRequest {
                            bucket: String::from(&CFG.s3_bucket),
                            key: k.to_string(),
                            body: Some(source[offset..offset_end].to_owned().into()),
                            ..Default::default()
                        }).await {
                            Ok(_) => log(LogLevel::Debug, &format!("Chunk ({}) succesfully uploaded to s3", k)),
                            Err(e) => log(LogLevel::Critical, &format!("Chunk ({}) could not be uploaded :: {}", k, e))
                        }
                    }
                    /* Send an update message to sync server */
                    let request = SendMessageRequest {
                        message_body: serde_json::to_string(&EventMessage {
                            c: Some(String::from(&CFG.client_id)),
                            e: Some(Event::Write(filename.to_string())),
                            d: Some(my_compare(&sig, &source[..], vec![0; CHUNK_SIZE]).unwrap()),
                        }).unwrap(),
                        message_group_id: Some(CFG.client_id.to_string()),
                        queue_url: CFG.sqs_downstream.clone(),
                        ..Default::default()
                    };
                    match sqs.send_message(request).await {
                        Ok(res) => log(LogLevel::Debug, &format!("Event(Create::{}) Message (id: {}) succesfully sent downstream",
                                                                filename, res.message_id.unwrap())),
                        Err(e) => log(LogLevel::Critical, &format!("Event(Create::{}) Message failed to send :: {}",
                                                                filename, e))
                    }
                    /* update metastore */
                    metastore.set(path, &source_sig);
                },
                None => {
                    /* key wasn't in there, must be a new file */
                    for (k, v) in source_sig.chunks.iter() {
                        /* check if chunks exists on s3 already */
                        if validate_chunk(*k, s3).await {
                            continue;
                        }                        let offset : usize = *v.values().next().unwrap().first().unwrap();
                        let offset_end : usize = if offset + CHUNK_SIZE <= source.len() { offset + CHUNK_SIZE } else { source.len() };
                        match s3.put_object(PutObjectRequest {
                            bucket: String::from(&CFG.s3_bucket),
                            key: k.to_string(),
                            body: Some(source[offset..offset_end].to_owned().into()),
                            ..Default::default()
                        }).await {
                            Ok(_) => log(LogLevel::Debug, &format!("Chunk ({}) succesfully uploaded to s3", k)),
                            Err(e) => log(LogLevel::Critical, &format!("Chunk ({}) could not be uploaded :: {}", k, e))
                        }
                    }
                    /* create an empty signature to fill since no signature was present */
                    let sig = Signature {
                        window: CHUNK_SIZE,
                        chunks: HashMap::new(),
                    };

                    /* Send an update message (the new file signature) to sync server */
                    let request = SendMessageRequest {
                        message_body: serde_json::to_string(&EventMessage {
                            c: Some(String::from(&CFG.client_id)),
                            e: Some(Event::Write(filename.to_string())),
                            d: Some(my_compare(&sig, &source[..], vec![0; CHUNK_SIZE]).unwrap()),
                        }).unwrap(),                                   
                        message_group_id: Some(CFG.client_id.to_string()),
                        queue_url: CFG.sqs_downstream.clone(),
                        ..Default::default()
                    };
                    match sqs.send_message(request).await {
                        Ok(res) => log(LogLevel::Debug, &format!("Event(Create::{}) Message (id: {}) succesfully sent downstream",
                                                                filename, res.message_id.unwrap())),
                        Err(e) => log(LogLevel::Critical, &format!("Event(Create::{}) Message failed to send :: {}",
                                                                filename, e))
                    }
                    metastore.set(path, &source_sig);
                }
            }
        },
        Err(e) => log(LogLevel::Warning, &format!("Could not read source file({})! :: {}", path, e))
    }
}

fn blacklist_file(path: &str, watcher: &mut INotifyWatcher) {
    let path_parent = Path::new(&path).parent().unwrap().to_str().unwrap();
    watcher.unwatch(&path);
    watcher.unwatch(&path_parent);
}

fn unblacklist_file(path: &str, watcher: &mut INotifyWatcher) {
    let path_parent = Path::new(&path).parent().unwrap().to_str().unwrap();
    watcher.watch(&path, RecursiveMode::NonRecursive);
    watcher.watch(&path_parent, RecursiveMode::NonRecursive);
}

pub async fn handle_event_message(em: &EventMessage, metastore: &mut PickleDb, watcher: &mut INotifyWatcher, s3: &S3Client) {
    match &em.e {
        Some(Event::Write(filename)) => {
            /* a file write requires the most work by far */
            let f = format!("{}{}", CFG.sync_dir, filename);
            /* blacklist the file */
            blacklist_file(&f, watcher);
            match &em.d {
                Some(d) => {
                    apply_file_write(&f, &d, metastore, s3).await;
                },
                None => {
                    return log(LogLevel::Critical, &format!("Received file ({}) create, but no chunks!", f));
                }
            }
            /* re-add file to watcher */
            unblacklist_file(&f, watcher);

        },
        Some(Event::Rename(f_filename, t_filename)) => {
            /* re-assemble the correct file paths for each filename */
            let f = format!("{}{}", CFG.sync_dir, f_filename);
            let t = format!("{}{}", CFG.sync_dir, t_filename);
            /* watcher should drop events for the files until done */
            blacklist_file(&f, watcher);
            blacklist_file(&t, watcher);

            let path_parent = Path::new(&t).parent().unwrap().to_str().unwrap();
            blacklist_file(&path_parent, watcher);
            println!("create_dir_all: {:?}", fs::create_dir_all(path_parent));
            unblacklist_file(&path_parent, watcher);

            /* rename the file */ 
            if let Err(e) = fs::rename(&f, &t) {
                return log(LogLevel::Critical, &format!("Could not rename file ({}->{}) :: {}", f, t, e));
            }
            
            log(LogLevel::Debug, &format!("Renaming file ({}->{})", f, t));

            match fs::metadata(&t) {
                Ok(md) => {
                    if md.is_dir() {
                        if let Err(e) = apply_dir_rename(&t, &f, metastore, watcher) {
                            return log(LogLevel::Critical, &format!("Could not apply directory rename ({}->{}) :: {}", f, t, e));
                        }
                    } else {
                        /* "simple" file rename */
                        apply_file_rename(&f, &t, metastore);
                    }
                },
                Err(e) => {
                    return log(LogLevel::Critical, &format!("Could not read metadata from file ({}) :: {}", t, e));
                }
            }
            unblacklist_file(&f, watcher);
            unblacklist_file(&t, watcher);
        },
        Some(Event::Remove(filename)) => {
            /* re-assemble the correct file paths for each filename */
            let f = format!("{}{}", CFG.sync_dir, filename);
            /* watcher should drop events for the file until done */
            blacklist_file(&f, watcher);

            log(LogLevel::Debug, &format!("Removing file ({})", f));

            /* rename the file, */
            match fs::metadata(&f) {
                Ok(md) => {
                    if md.is_dir() {
                        /* remove all the stuffs in the dir */
                        /* BUT, have to go through it and remove each file from the metastore */
                        /* dirs do not have entries in the metastore, but they may have files */

                        /* need to do the blacklisting within `apply_dir_removal` too */
                        if let Err(e) = apply_dir_removal(&f, metastore, watcher) {
                            return log(LogLevel::Critical, &format!("Could not apply directory removal ({}) :: {}", f, e));
                        }
                        if let Err(e) = fs::remove_dir_all(&f) {
                            return log(LogLevel::Critical, &format!("Could not remove all from directory ({}) :: {}", f, e));
                        }
                    } else {
                        /* only delete the file */
                        if let Err(e) = fs::remove_file(&f) {
                            return log(LogLevel::Critical, &format!("Could not remove file ({}) :: {}", f, e));
                        }
                        /* get rid of the file from the metastore */
                        metastore.rem(&f);
                    }
                },
                Err(e) => {
                    return log(LogLevel::Critical, &format!("Could not read metadata from file ({}) :: {}", f, e));
                }
            }
            unblacklist_file(&f, watcher);
        },
        _ => {}
    }
}

fn write_chunk_from_source(path: &str, id: u32, offset_u: usize, offset: usize, file: &mut File,
     sig: &mut Signature, source: &[u8], modify_sig: bool) -> usize {
    let offset_end: usize = if offset_u + CHUNK_SIZE <= source.len() { offset_u + CHUNK_SIZE } else { source.len() };
    if let Err(e) = file.write_all(&source[offset_u..offset_end]) {
        log(LogLevel::Critical, &format!("Could not write from Source ({}..{}) from s3 for file ({}) :: {}",
                offset_u, offset_end, path, e));
        return 0
    }
    if modify_sig {
        /* add to the new signature */
        let mut blake2 = [0; BLAKE2_SIZE];
        blake2.clone_from_slice(blake2_rfc::blake2b::blake2b(BLAKE2_SIZE, &[],
                                    &source[offset_u..offset_end]).as_bytes());
        sig.chunks.entry(id)
                        .or_insert(HashMap::new())
                        .entry(Blake2b(blake2))
                        .or_insert(Vec::new())
                        .push(offset);
    }
    offset_end - offset_u
}

async fn write_chunk_from_s3(path: &str, id: u32, offset: usize, file: &mut File, sig: &mut Signature,
     block_map: &mut HashMap<u32, Vec<u8>>, s3: &S3Client, modify_sig: bool) -> usize {
    /* chunk must be retrieved from S3... ideally, there would be a local chunk
        mapping which would allow reading chunks from other files (if they are present)
        instead of making requests to S3 (much faster) */
    /* This is also where I will cache chunks */
    if let Some(buf) = block_map.get(&id) {
        if let Err(e) = file.write_all(&buf) {
            log(LogLevel::Critical, &format!("Could not write all to file ({}) :: {}", path, e));
            return 0
        }
        if modify_sig {
            sig.chunks.get_mut(&id).unwrap()
                .values_mut().next().unwrap()
                .push(offset);
        }
        buf.len();
    }
    match s3.get_object(GetObjectRequest {
        bucket: String::from(&CFG.s3_bucket),
        key: id.to_string(),
        ..Default::default()
    }).await {
        Ok(obj) => {
            let mut block_buf = Vec::new();
            let mut body = obj.body.unwrap().into_async_read();
            /* if this fails, the file is most likely hosed anyway */
            let cnt = io::copy(&mut body, &mut block_buf).await.unwrap() as usize;
            if let Err(e) = file.write_all(&block_buf) {
                log(LogLevel::Critical, &format!("Could not write from Chunk ({}) to file ({}) :: {}",
                    id, path, e));
            }
            /* cache the block in case it's used again later  */
            block_map.insert(id, block_buf.clone());
            if modify_sig {
                /* add to the new signature */
                let mut blake2 = [0; BLAKE2_SIZE];
                blake2.clone_from_slice(blake2_rfc::blake2b::blake2b(BLAKE2_SIZE, &[],
                                        &block_buf).as_bytes());
                sig.chunks.entry(id)
                    .or_insert(HashMap::new())
                    .entry(Blake2b(blake2))
                    .or_insert(Vec::new())
                    .push(offset);
            }
            cnt
        }
        Err(e) => {
            /* either the chunk wasn't present or the s3client/network issue */
            log(LogLevel::Critical, &format!("Could not receive Chunk ({}) from s3 for file ({}) :: {}",
                id, path, e));
            0
        }
    }
}

pub async fn apply_file_write(path: &str, delta: &MyDelta, metastore: &mut PickleDb, s3: &S3Client) {
    /* This is naive in terms of memory, but I'm going to memoize S3 chunks...
        in other words, map chunk id to the data */
    let mut block_map: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut new_sig: Signature = Signature { window: CHUNK_SIZE, chunks: HashMap::new() };
    let mut i = 0;
    /* As it stands, the easiest way to do this is to basically read file into mem */
    match fs::read(path) {
        Ok(source) => {
            match fs::OpenOptions::new().write(true).open(path) {
                Ok(mut file) => {
                    match metastore.get::<Signature>(path) {
                        Some(sig) => {
                            /* found the signature, use it to construct the file, make a new signature */
                            for chunk_id in &delta.blocks {
                                /* check that chunk is present in the signature, giving you the offset,
                                    otherwise, chunk must be fetched from S3 (and cached), the signature should
                                    be updated here as well */
                                match sig.chunks.get(&chunk_id) {
                                    /* retrieved is a hashmap ({b2b -> offset}) */
                                    Some(cidx) => {
                                        /* since the chunk is present, there will be an offset for it */
                                        let offset_u = *cidx.values().next().unwrap().first().unwrap();
                                        i += write_chunk_from_source(path, *chunk_id, offset_u, i, &mut file, &mut new_sig, &source, true);
                                    },
                                    None => {
                                        i += write_chunk_from_s3(path, *chunk_id, i, &mut file, &mut new_sig, &mut block_map, s3, true).await;
                                    }
                                };
                            }     
                        },
                        None => {
                            /* file not in metastore, but might be on disk? */
                        }
                    };
                },
                Err(e) => {
                    log(LogLevel::Critical, &format!("Could not open file ({}) for writing :: {}", path, e));
                }
            };
            /* get the signature from metastore, if not in metastore
                Q: would ever be case where not in metastore (but on disk) - No??? */
            /* work that needs to be done is done, now we have to update the signature,
                unfortunately, the only way with what we have is to read it again (bleh) */
        },
        Err(_) => {
            /* there was no pre-existing file (or it couldn't be read), so the only way to retrieve chunks is s3,
                need to create directories if needed too */
            let path_parent = Path::new(&path).parent().unwrap().to_str().unwrap();
            fs::create_dir_all(path_parent);
            match fs::OpenOptions::new().write(true).create(true).open(path) {
                Ok(mut file) => {   
                    log(LogLevel::Debug, &format!("Creating new file ({})", path));
                    // log(LogLevel::Critical, &format!("Could not open file ({}) for reading :: {}", path, e));
                    for chunk_id in &delta.blocks {
                        i += write_chunk_from_s3(path, *chunk_id, i, &mut file, &mut new_sig, &mut block_map, s3, true).await;
                    }
                },
                Err(e) => {
                    log(LogLevel::Critical, &format!("Could not open file ({}) for writing :: {}", path, e));
                }
            };
        }
    };
    metastore.set(path, &new_sig);
}

pub fn apply_file_rename(path_f: &str, path_t: &str, metastore: &mut PickleDb) {
    /* replace the f-sig with the t-sig */
    let block_buf = vec![0; CHUNK_SIZE];
    let sig : Signature;
    /* remove the old entry, or compute hash of t if no entry */
    if let Some(f_sig) = metastore.get(&path_f) {
        metastore.rem(&path_f);
        sig = f_sig;
    } else {
        /* if can't open file (maybe it got deleted), just log and continue */
        match fs::read(path_t) {
            Ok(source) => {
                /* have the signature now */
                sig = signature(&source[..], block_buf).unwrap();
            },
            Err(e) => {
                return log(LogLevel::Warning, &format!("Could not read source file ({}) :: {}", path_t, e));
            }
        };
    };
    /* update t in metastore (doesn't matter if t was already there, file is already gone) */
    metastore.set(&path_t, &sig);
}

pub fn apply_dir_rename(path: &str, path_old: &str, metastore: &mut PickleDb, watcher: &mut INotifyWatcher) -> Result<()> {
    match fs::read_dir(path) {
        Ok(ents) => {
            for ent in ents {
                /* unwrap the entry since read dir returns them as results */
                let entry = ent?;
                let entry_path = entry.path();
                let path_str = entry_path.clone().into_os_string().into_string().unwrap();
                let filename = entry_path.as_path().file_name().unwrap().to_str().unwrap();
                if entry_path.is_dir() {
                    /* recurse on a dir */
                    apply_dir_rename(&path_str, path_old, metastore, watcher)?;
                } else {
                    /* remove and re-insert the updated entry! */
                    let old_path = format!("{}/{}", path_old, filename);
                    if let Some(sig) = metastore.get::<Signature>(&old_path) {
                        /* valid entry, put it back with the modified path */
                        /* pathing stuff really should be accomplished using `Path`s,
                            but I was completely unaware of them until too late */
                        metastore.rem(&old_path);
                        metastore.set(&format!("{}/{}", path, filename), &sig);
                    }
                }
                watcher.watch(path, RecursiveMode::NonRecursive);
            }
        },
        Err(e) => {
            log(LogLevel::Critical, &format!("Could not read dir ({}) :: {}", path, e));
        }
    }
    Ok(())
}

pub fn apply_dir_removal(path: &str, metastore: &mut PickleDb, watcher: &mut INotifyWatcher) -> Result<()> {
    match fs::read_dir(path) {
        Ok(ents) => {
            for ent in ents {
                /* unwrap the entry since read dir returns them as results */
                let entry = ent?;
                let entry_path = entry.path();
                let path_str = entry_path.clone().into_os_string().into_string().unwrap();
                /* blacklist the file/dir */
                blacklist_file(&path_str, watcher);
                if entry_path.is_dir() {
                    /* recurse on a dir */
                    apply_dir_removal(&path_str, metastore, watcher)?;
                } else {
                    /* remove the entry! */
                    metastore.rem(&path_str);
                }
            }
        },
        Err(e) => {
            log(LogLevel::Critical, &format!("Could not read dir ({}) :: {}", path, e));
        }
    }
    Ok(())
}