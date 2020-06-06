use std::{string::String, collections::HashSet};
use std::sync::mpsc::{Sender};
use my_notify::{DebouncedEvent};
use crossbeam::crossbeam_channel::{Receiver};

use crate::event::*;
use crate::log::*;

/* handle file event accordingly, form new event, forward the msg */
pub fn handle_inotify_event(e: DebouncedEvent, tx: &Sender<Event>, notify_rx: &Receiver<DebouncedEvent>) {
        match e {
            DebouncedEvent::Create(f) => {
                match filter_path(f.clone().into_os_string().into_string().unwrap()) {
                    Some(p) => {
                        log(LogLevel::Info, &format!("Handling file create: {}", p));
                        tx.send(Event::Write(p));
                    },
                    None => {
                        log(LogLevel::Info, &format!("Skipping file create: {:?}", f));
                    }
                }
            },
            DebouncedEvent::Write(f) => {
                match filter_path(f.clone().into_os_string().into_string().unwrap()) {
                    Some(p) => {
                        log(LogLevel::Info, &format!("Handling file write: {}", p));
                        tx.send(Event::Write(p));
                    },
                    None => {
                        log(LogLevel::Info, &format!("Skipping file write: {:?}", f));
                    }
                }                    
            },
            DebouncedEvent::Rename(f, t) => {
                /* A rename to hidden file is considered a remove,
                    while a hidden file renamed to normal is ignored 
                    since it will be handled normally next time written */
                match (filter_path(f.clone().into_os_string().into_string().unwrap()),
                        filter_path(t.clone().into_os_string().into_string().unwrap())) {
                    /* Normal rename (move) */
                    (Some(pf), Some(pt)) => {
                        log(LogLevel::Info, &format!("Handling file rename: {} -> {}", pf, pt));
                        /* need to get the duplicate remove event out for the rename,
                            very janky, but I'm fairly certain the remove event will always immediately
                            follow the rename event */
                        notify_rx.recv();
                        tx.send(Event::Rename(pf, pt));
                    },
                    /* Normal to hidden rename (remove) */
                    (Some(pf), None) => {
                        log(LogLevel::Info, &format!("Handling file rename (removal): {} ", pf));
                        tx.send(Event::Remove(pf));
                    },
                    /* ignore hidden to normal and hidden to hidden */
                    _ => {
                        log(LogLevel::Info, &format!("Skipping file rename: {:?} -> {:?}", f, t));
                    }
                }    
            },
            DebouncedEvent::Remove(f) => {
                match filter_path(f.clone().into_os_string().into_string().unwrap()) {
                    Some(p) => {
                        log(LogLevel::Info, &format!("Handling file remove: {}", p));
                        tx.send(Event::Remove(p));
                    },
                    None => {
                        log(LogLevel::Info, &format!("Skipping file remove: {:?}", f));
                    }
                }
            },
            _ => {
                /* this is some other debounced type from inotify we don't care about */
            },
        };
}

/* return the path if valid or nothing if invalid */
/* TODO: this could probably accept a filter 'function'  */
fn filter_path(path: String) -> Option<String> {
    match path.rfind('/') {
        Some(idx) => {
            let res = path[(idx + 1)..].starts_with('/');
            // println!("path: {}, res: {}", &path[(idx + 1)..], res);
            match !res {
                true => Some(path),
                false => None
            }
        },
        None => None
    }
}

fn check_blacklist(f: Option<String>, fb: &HashSet<String>) -> (Option<String>, bool) {
    match f {
        /* only check list if f is a valid path */
        Some(x) => {
            match fb.get(&x) {
                /* f is on the blacklist */
                Some(y) => (Some(x), true),
                /* f is not on the blacklist */
                None => (Some(x), false)
            }
        },
        None => (None, false)
    }
}