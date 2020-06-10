//! An implementation of an rsync-like protocol (not compatible with
//! rsync), in pure Rust.
//!
//! ```
//! extern crate rand;
//! extern crate rustsync;
//! use rustsync::*;
//! use rand::Rng;
//! fn main() {
//!   // Create 4 different random strings first.
//!   let chunk_size = 1000;
//!   let a = rand::thread_rng()
//!           .gen_ascii_chars()
//!           .take(chunk_size)
//!           .collect::<String>();
//!   let b = rand::thread_rng()
//!           .gen_ascii_chars()
//!           .take(50)
//!           .collect::<String>();
//!   let b_ = rand::thread_rng()
//!           .gen_ascii_chars()
//!           .take(100)
//!           .collect::<String>();
//!   let c = rand::thread_rng()
//!           .gen_ascii_chars()
//!           .take(chunk_size)
//!           .collect::<String>();
//!
//!   // Now concatenate them in two different ways.
//!
//!   let mut source = a.clone() + &b + &c;
//!   let mut modified = a + &b_ + &c;
//!
//!   // Suppose we want to download `modified`, and we already have
//!   // `source`, which only differs by a few characters in the
//!   // middle.
//!
//!   // We first have to choose a block size, which will be recorded
//!   // in the signature below. Blocks should normally be much bigger
//!   // than this in order to be efficient on large files.
//!
//!   let block = [0; 32];
//!
//!   // We then create a signature of `source`, to be uploaded to the
//!   // remote machine. Signatures are typically much smaller than
//!   // files, with just a few bytes per block.
//!
//!   let source_sig = signature(source.as_bytes(), block).unwrap();
//!
//!   // Then, we let the server compare our signature with their
//!   // version.
//!
//!   let comp = compare(&source_sig, modified.as_bytes(), block).unwrap();
//!
//!   // We finally download the result of that comparison, and
//!   // restore their file from that.
//!
//!   let mut restored = Vec::new();
//!   restore_seek(&mut restored, std::io::Cursor::new(source.as_bytes()), vec![0; 1000], &comp).unwrap();
//!   assert_eq!(&restored[..], modified.as_bytes())
//! }
//! ```

extern crate adler32;
extern crate blake2_rfc;
extern crate futures;
#[cfg(test)]
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg(test)]
extern crate tokio_core;
extern crate tokio_io;

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::io::{write_all, WriteAll};
use futures::{Async, Future, Poll};

pub const BLAKE2_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Blake2b(pub [u8; BLAKE2_SIZE]);

impl std::borrow::Borrow<[u8]> for Blake2b {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// The "signature" of the file, which is essentially a
/// content-indexed description of the blocks in the file.
pub struct Signature {
    pub window: usize,
    pub chunks: HashMap<u32, HashMap<Blake2b, Vec<usize>>>
}

impl PartialEq for Signature {
    fn eq(&self, other: &Self) -> bool {
        let mut filtered_self = self.chunks.clone();
        let mut filtered_other = other.chunks.clone();
        filtered_self.remove(&1);
        filtered_other.remove(&1);

        (self.window == other.window) &&
        (filtered_self.len() == filtered_other.len()) &&
        (filtered_self == filtered_other)
    }
}

/// Create the "signature" of a file, essentially a content-indexed
/// map of blocks. The first step of the protocol is to run this
/// function on the "source" (the remote file when downloading, the
/// local file while uploading).
pub fn signature<R: Read, B: AsRef<[u8]>+AsMut<[u8]>>(mut r: R, mut block: B) -> Result<Signature, std::io::Error> {
    let mut chunks = HashMap::new();

    let mut i = 0;
    let block = block.as_mut();
    let mut eof = false;
    while !eof {
        let mut j = 0;
        while j < block.len() {
            let r = r.read(&mut block[j..])?;
            if r == 0 {
                eof = true;
                break;
            }
            j += r
        }
        let block = &block[..j];
        let hash = adler32::RollingAdler32::from_buffer(block);
        let mut blake2 = [0; BLAKE2_SIZE];
        blake2.clone_from_slice(blake2_rfc::blake2b::blake2b(BLAKE2_SIZE, &[], &block).as_bytes());
        // println!("{:?} {:?}", block, blake2);
        chunks
            .entry(hash.hash())
            .or_insert(HashMap::new())
            .entry(Blake2b(blake2))
            .or_insert(Vec::new())
            .push(i);

        i += block.len()
    }

    Ok(Signature {
        window: block.len(),
        chunks
    })
}

// pub struct ReadBlock<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> {
//     block: Option<(R, B)>,
//     first: usize,
// }

// impl<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> ReadBlock<R, B> {
//     fn new(r: R, block: B) -> Self {
//         ReadBlock {
//             block: Some((r, block)),
//             first: 0,
//         }
//     }
// }

// impl<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> Future for ReadBlock<R, B> {
//     type Item = (R, B, usize);
//     type Error = std::io::Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         loop {
//             if let Some((mut r, mut block)) = self.block.take() {
//                 let n = {
//                     let block = block.as_mut();
//                     if self.first == block.len() {
//                         0
//                     } else {
//                         match r.read(&mut block[self.first..]) {
//                             Ok(n) => n,
//                             Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
//                                 return Ok(Async::NotReady)
//                             }
//                             Err(e) => return Err(e),
//                         }
//                     }
//                 };
//                 if n == 0 {
//                     return Ok(Async::Ready((r, block, self.first)));
//                 } else {
//                     self.first += n;
//                     self.block = Some((r, block));
//                 }
//             } else {
//                 panic!("future polled after completion")
//             }
//         }
//     }
// }

// pub struct WriteBlock<W: AsyncWrite, B: AsRef<[u8]>> {
//     block: Option<(W, B)>,
//     first: usize,
//     len: usize,
// }

// impl<W: AsyncWrite, B: AsRef<[u8]>> WriteBlock<W, B> {
//     fn new(w: W, block: B, first: usize, len: usize) -> Self {
//         WriteBlock {
//             block: Some((w, block)),
//             first,
//             len,
//         }
//     }
// }

// impl<W: AsyncWrite, B: AsRef<[u8]>> Future for WriteBlock<W, B> {
//     type Item = (W, B);
//     type Error = std::io::Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         loop {
//             if let Some((mut w, block)) = self.block.take() {
//                 match w.write(&block.as_ref()[self.first..self.len]) {
//                     Ok(n) => {
//                         self.first += n;
//                         if self.first >= self.len {
//                             return Ok(Async::Ready((w, block)));
//                         } else {
//                             self.block = Some((w, block))
//                         }
//                     }
//                     Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
//                         return Ok(Async::NotReady)
//                     }
//                     Err(e) => return Err(e),
//                 }
//             } else {
//                 panic!("future polled after completion")
//             }
//         }
//     }
// }

// pub struct FutureSignature<R: AsyncRead, B:AsRef<[u8]>+AsMut<[u8]>> {
//     chunks: HashMap<u32, HashMap<Blake2b, usize>>,
//     i: usize,
//     eof: bool,
//     state: Option<ReadBlock<R, B>>,
// }

// impl<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> Future for FutureSignature<R, B> {
//     type Item = (R, Signature);
//     type Error = std::io::Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         loop {
//             if let Some(mut reading) = self.state.take() {
//                 if let Async::Ready((r, block, len)) = reading.poll()? {
//                     {
//                         let block_ = block.as_ref();
//                         self.eof = len < block_.len();
//                         let b = &block_[..len];
//                         let hash = adler32::RollingAdler32::from_buffer(b);
//                         let mut blake2 = [0; BLAKE2_SIZE];
//                         blake2.clone_from_slice(
//                             blake2_rfc::blake2b::blake2b(BLAKE2_SIZE, &[], &b).as_bytes(),
//                         );
//                         self.chunks
//                             .entry(hash.hash())
//                             .or_insert(HashMap::new())
//                             .insert(Blake2b(blake2), self.i);
//                         self.i += block_.len();
//                     }
//                     if self.eof {
//                         return Ok(Async::Ready((
//                             r,
//                             Signature {
//                                 chunks: std::mem::replace(&mut self.chunks, HashMap::new()),
//                                 window: block.as_ref().len(),
//                             },
//                         )));
//                     } else {
//                         self.state = Some(ReadBlock::new(r, block))
//                     }
//                 } else {
//                     self.state = Some(reading);
//                     return Ok(Async::NotReady);
//                 }
//             }
//         }
//     }
// }

// /// This is the same as [`signature`](fn.signature.html), except that
// /// this function reads the input source asynchronously.
// pub fn signature_fut<R: AsyncRead, B:AsRef<[u8]>+AsMut<[u8]>>(r: R, b: B) -> FutureSignature<R, B> {
//     FutureSignature {
//         state: Some(ReadBlock::new(r, b)),
//         i: 0,
//         eof: false,
//         chunks: HashMap::new(),
//     }
// }

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Block {
    FromSource(u64),
    Literal(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MyBlock {
    FromSource(u64),
    Literal(u32),
}

struct State {
    result: Vec<Block>,
    block_oldest: usize,
    block_len: usize,
    pending: Vec<u8>,
}

impl State {
    fn new() -> Self {
        State {
            result: Vec::new(),
            block_oldest: 0,
            block_len: 1,
            pending: Vec::new(),
        }
    }
}
#[derive(Debug)]
struct MyState {
    result: Vec<u32>,
    block_oldest: usize,
    block_len: usize,
    pending: Vec<u8>,
}

impl MyState {
    fn new() -> Self {
        MyState {
            result: Vec::new(),
            block_oldest: 0,
            block_len: 1,
            pending: Vec::new(),
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
/// The result of comparing two files
pub struct Delta {
    /// Description of the new file in terms of blocks.
    pub blocks: Vec<Block>,
    /// Size of the window.
    pub window: usize,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
/// The result of comparing two files
pub struct MyDelta {
    /// Description of the new file in terms of blocks.
    pub blocks: Vec<u32>,
    /// Size of the window.
    pub window: usize,
}

/// Compare a signature with an existing file. This is the second step
/// of the protocol, `r` is the local file when downloading, and the
/// remote file when uploading.
///
/// `block` must be a buffer the same size as `sig.window`.
// pub fn compare<R: Read, B:AsRef<[u8]>+AsMut<[u8]>>(sig: &Signature, mut r: R, mut block: B) -> Result<Delta, std::io::Error> {
//     let mut st = State::new();
//     let block = block.as_mut();
//     assert_eq!(block.len(), sig.window);
//     while st.block_len > 0 {
//         let mut hash = {
//             let mut j = 0;
//             let block = {
//                 while j < sig.window {
//                     let r = r.read(&mut block[..])?;
//                     if r == 0 {
//                         break;
//                     }
//                     j += r
//                 }
//                 st.block_oldest = 0;
//                 st.block_len = j;
//                 &block[..j]
//             };
//             adler32::RollingAdler32::from_buffer(block)
//         };

//         // Starting from the current block (with hash `hash`), find
//         // the next block with a hash that appears in the signature.
//         loop {
//             if matches(&mut st, sig, &block, &hash) {
//                 break;
//             }
//             // The blocks are not equal. Move the hash by one byte
//             // until finding an equal block.
//             let oldest = block[st.block_oldest];
//             hash.remove(st.block_len, oldest);
//             let r = r.read(&mut block[st.block_oldest..st.block_oldest + 1])?;
//             if r > 0 {
//                 // If there are still bytes to read, update the hash.
//                 hash.update(block[st.block_oldest]);
//             } else if st.block_len > 0 {
//                 // Else, just shrink the window, so that the current
//                 // block's blake2 hash can be compared with the
//                 // signature.
//                 st.block_len -= 1;
//             } else {
//                 // We're done reading the file.
//                 break;
//             }
//             st.pending.push(oldest);
//             st.block_oldest = (st.block_oldest + 1) % sig.window;
//         }
//         if !st.pending.is_empty() {
//             // We've reached the end of the file, and have never found
//             // a matching block again.
//             st.result.push(Block::Literal(std::mem::replace(
//                 &mut st.pending,
//                 Vec::new(),
//             )))
//         }
//     }
//     Ok(Delta {
//         blocks: st.result,
//         window: sig.window,
//     })
// }

pub fn my_compare<R: Read, B:AsRef<[u8]>+AsMut<[u8]>>(sig: &Signature, mut r: R, mut block: B) -> Result<MyDelta, std::io::Error> {
    let mut st = MyState::new();
    let block = block.as_mut();
    assert_eq!(block.len(), sig.window);
    while st.block_len > 0 {
        let hash = {
            let mut j = 0;
            let block = {
                while j < sig.window {
                    let r = r.read(&mut block[..])?;
                    if r == 0 {
                        break;
                    }
                    j += r
                }
                st.block_oldest = 0;
                st.block_len = j;
                &block[..j]
            };
            adler32::RollingAdler32::from_buffer(block)
        };

        // Starting from the current block (with hash `hash`), find
        // the next block with a hash that appears in the signature.
        loop {
            if my_matches(&mut st, sig, &block, &hash) {
                break;
            } 
            else {
                st.result.push(hash.hash()); 
                break;
            }
        }
        if !st.pending.is_empty() {
            // We've reached the end of the file, and have never found
            // a matching block again.
            std::mem::replace(
                    &mut st.pending,
                    Vec::new(),
            );
            st.result.push(hash.hash());
        }
    }
    Ok(MyDelta {
        blocks: st.result,
        window: sig.window,
    })
}

// pub struct FutureCompare<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> {
//     state: Option<CompareState<R, B>>,
//     st: State,
// }

// enum CompareState<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> {
//     ReadBlock {
//         readblock: ReadBlock<R, B>,
//         sig: Signature,
//     },
//     FindNext {
//         hash: adler32::RollingAdler32,
//         r: R,
//         block: B,
//         sig: Signature,
//     },
//     FindNextRead {
//         hash: adler32::RollingAdler32,
//         reading: ReadBlock<R, [u8; 1]>,
//         block: B,
//         sig: Signature,
//     },
//     EndLiteralBlock {
//         r: R,
//         block: B,
//         sig: Signature,
//     },
// }

// impl<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>> Future for FutureCompare<R, B> {
//     type Item = (R, Signature, Delta);
//     type Error = std::io::Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         loop {
//             match self.state.take() {
//                 Some(CompareState::ReadBlock { mut readblock, sig }) => {
//                     if let Async::Ready((r, block, len)) = readblock.poll()? {
//                         self.st.block_oldest = 0;
//                         self.st.block_len = len;
//                         let hash = adler32::RollingAdler32::from_buffer(&block.as_ref()[..len]);
//                         self.state = Some(CompareState::FindNext {
//                             hash,
//                             block,
//                             r,
//                             sig,
//                         });
//                     } else {
//                         self.state = Some(CompareState::ReadBlock { readblock, sig });
//                         return Ok(Async::NotReady);
//                     }
//                 }
//                 Some(CompareState::FindNext {
//                     mut hash,
//                     r,
//                     block,
//                     sig,
//                 }) => {
//                     if matches(&mut self.st, &sig, block.as_ref(), &hash) {
//                         self.state = Some(CompareState::EndLiteralBlock { r, block, sig })
//                     } else {
//                         let oldest = block.as_ref()[self.st.block_oldest];
//                         hash.remove(self.st.block_len, oldest);
//                         self.state = Some(CompareState::FindNextRead {
//                             hash,
//                             reading: ReadBlock::new(r, [0; 1]),
//                             block,
//                             sig,
//                         })
//                     }
//                 }
//                 Some(CompareState::FindNextRead {
//                     mut hash,
//                     mut reading,
//                     mut block,
//                     sig,
//                 }) => {
//                     if let Async::Ready((r, b, len)) = reading.poll()? {
//                         if len > 0 || self.st.block_len > 0 {
//                             let oldest = block.as_ref()[self.st.block_oldest];
//                             if len > 0 {
//                                 block.as_mut()[self.st.block_oldest] = b[0];
//                                 hash.update(b[0])
//                             } else {
//                                 self.st.block_len -= 1
//                             }

//                             self.st.pending.push(oldest);
//                             self.st.block_oldest = (self.st.block_oldest + 1) % sig.window;
//                             self.state = Some(CompareState::FindNext {
//                                 hash,
//                                 r,
//                                 block,
//                                 sig,
//                             })
//                         } else {
//                             self.state = Some(CompareState::EndLiteralBlock { r, block, sig })
//                         }
//                     } else {
//                         self.state = Some(CompareState::FindNextRead {
//                             hash,
//                             reading,
//                             block,
//                             sig,
//                         })
//                     }
//                 }
//                 Some(CompareState::EndLiteralBlock { r, block, sig }) => {
//                     if !self.st.pending.is_empty() {
//                         // We've reached the end of the file, and have never found
//                         // a matching block again.
//                         self.st.result.push(Block::Literal(std::mem::replace(
//                             &mut self.st.pending,
//                             Vec::new(),
//                         )))
//                     }
//                     if self.st.block_len > 0 {
//                         self.state = Some(CompareState::ReadBlock {
//                             readblock: ReadBlock::new(r, block),
//                             sig,
//                         })
//                     } else {
//                         let window = sig.window;
//                         return Ok(Async::Ready((
//                             r,
//                             sig,
//                             Delta {
//                                 blocks: std::mem::replace(&mut self.st.result, Vec::new()),
//                                 window,
//                             },
//                         )));
//                     }
//                 }
//                 None => panic!(""),
//             }
//         }
//     }
// }

/// Same as [`compare`](fn.compare.html), except that this function
/// reads the file asynchronously.
// pub fn compare_fut<R: AsyncRead, B: AsRef<[u8]>+AsMut<[u8]>>(sig: Signature, r: R, block: B) -> FutureCompare<R, B> {
//     assert_eq!(block.as_ref().len(), sig.window);
//     FutureCompare {
//         state: Some(CompareState::ReadBlock {
//             readblock: ReadBlock::new(r, block),
//             sig,
//         }),
//         st: State::new(),
//     }
// }

// fn matches(st: &mut State, sig: &Signature, block: &[u8], hash: &adler32::RollingAdler32) -> bool {
//     if let Some(h) = sig.chunks.get(&hash.hash()) {
//         let blake2 = {
//             let mut b = blake2_rfc::blake2b::Blake2b::new(BLAKE2_SIZE);
//             if st.block_oldest + st.block_len > sig.window {
//                 b.update(&block[st.block_oldest..]);
//                 b.update(&block[..(st.block_oldest + st.block_len) % sig.window]);
//             } else {
//                 b.update(&block[st.block_oldest..st.block_oldest + st.block_len])
//             }
//             b.finalize()
//         };

//         if let Some(&index) = h.get(blake2.as_bytes()) {
//             // Matching hash found! If we have non-matching
//             // material before the match, add it.
//             if !st.pending.is_empty() {
//                 st.result.push(Block::Literal(std::mem::replace(
//                     &mut st.pending,
//                     Vec::new(),
//                 )));
//             }
//             st.result.push(Block::FromSource(index as u64));
//             return true;
//         }
//     }
//     false
// }

fn my_matches(st: &mut MyState, sig: &Signature, block: &[u8], hash: &adler32::RollingAdler32) -> bool {
    let hashed = &hash.hash();
    if let Some(h) = sig.chunks.get(hashed) {
        let blake2 = {
            let mut b = blake2_rfc::blake2b::Blake2b::new(BLAKE2_SIZE);
            if st.block_oldest + st.block_len > sig.window {
                b.update(&block[st.block_oldest..]);
                b.update(&block[..(st.block_oldest + st.block_len) % sig.window]);
            } else {
                b.update(&block[st.block_oldest..st.block_oldest + st.block_len])
            }
            b.finalize()
        };

        if h.get(blake2.as_bytes()).is_some() {
            // Matching hash found! If we have non-matching
            // material before the match, add it.
            if !st.pending.is_empty() {
                st.result.push(adler32::RollingAdler32::from_buffer(&st.pending).hash());
                std::mem::replace(
                    &mut st.pending,
                    Vec::new(),
                );
            }
            st.result.push(*hashed);
            return true;
        }
    }
    false
}

// / Restore a file, using a "delta" (resulting from
// / [`compare`](fn.compare.html))
// pub fn restore<W: Write>(mut w: W, s: &[u8], delta: &Delta) -> Result<(), std::io::Error> {
//     for d in delta.blocks.iter() {
//         match *d {
//             Block::FromSource(i) => {
//                 let i = i as usize;
//                 if i + delta.window <= s.len() {
//                     w.write(&s[i..i + delta.window])?
//                 } else {
//                     w.write(&s[i..])?
//                 }
//             }
//             Block::Literal(ref l) => w.write(l)?,
//         };
//     }
//     Ok(())
// }

// / Same as [`restore`](fn.restore.html), except that this function
// / uses a seekable, readable stream instead of the entire file in a
// / slice.
// /
// / `buf` must be a buffer the same size as `sig.window`.
// pub fn restore_seek<W: Write, R: Read + Seek, B: AsRef<[u8]>+AsMut<[u8]>>(
//     mut w: W,
//     mut s: R,
//     mut buf: B,
//     delta: &Delta,
// ) -> Result<(), std::io::Error> {
//     let buf = buf.as_mut();

//     for d in delta.blocks.iter() {
//         match *d {
//             Block::FromSource(i) => {
//                 s.seek(SeekFrom::Start(i as u64))?;
//                 // fill the buffer from r.
//                 let mut n = 0;
//                 loop {
//                     let r = s.read(&mut buf[n..delta.window])?;
//                     if r == 0 {
//                         break;
//                     }
//                     n += r
//                 }
//                 // write the buffer to w.
//                 let mut m = 0;
//                 while m < n {
//                     m += w.write(&buf[m..n])?;
//                 }
//             }
//             Block::Literal(ref l) => {
//                 w.write(l)?;
//             }
//         }
//     }
//     Ok(())
// }

// pub struct FutureRestore<W: AsyncWrite, R: Read + Seek, B: AsMut<[u8]> + AsRef<[u8]>> {
//     state: Option<RestoreState<W, R, B>>,
//     delta: Delta,
//     delta_pos: usize,
// }

// enum RestoreState<W: AsyncWrite, R: Read + Seek, B: AsMut<[u8]> + AsRef<[u8]>> {
//     Delta {
//         w: W,
//         r: R,
//         buf: B,
//     },
//     WriteBuf {
//         write: WriteBlock<W, B>,
//         r: R,
//     },
//     WriteVec {
//         write: WriteAll<W, Vec<u8>>,
//         r: R,
//         buf: B,
//     },
// }

// /// Same as [`restore_seek`](fn.restore_seek.html), except that this
// /// function writes its output asynchronously.
// pub fn restore_seek_fut<W: AsyncWrite, R: Read + Seek, B: AsMut<[u8]> + AsRef<[u8]>>(
//     w: W,
//     r: R,
//     buf: B,
//     delta: Delta,
// ) -> FutureRestore<W, R, B> {
//     FutureRestore {
//         state: Some(RestoreState::Delta { w, r, buf }),
//         delta,
//         delta_pos: 0,
//     }
// }

// impl<W: AsyncWrite, R: Read + Seek, B: AsMut<[u8]> + AsRef<[u8]>> Future
//     for FutureRestore<W, R, B> {
//     type Item = (W, R, Delta);
//     type Error = std::io::Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         loop {
//             match self.state.take() {
//                 Some(RestoreState::Delta { w, mut r, mut buf }) => {
//                     if self.delta_pos >= self.delta.blocks.len() {
//                         return Ok(Async::Ready((
//                             w,
//                             r,
//                             std::mem::replace(&mut self.delta, Delta::default()),
//                         )));
//                     }
//                     match self.delta.blocks[self.delta_pos] {
//                         Block::FromSource(i) => {
//                             r.seek(SeekFrom::Start(i as u64))?;
//                             // fill the buffer from r.
//                             let mut n = 0;
//                             {
//                                 let buf_ = buf.as_mut();
//                                 loop {
//                                     let k = r.read(&mut buf_[n..self.delta.window])?;
//                                     if k == 0 {
//                                         break;
//                                     }
//                                     n += k
//                                 }
//                             }
//                             // write the buffer to w.
//                             self.state = Some(RestoreState::WriteBuf {
//                                 write: WriteBlock::new(w, buf, 0, n),
//                                 r,
//                             })
//                         }
//                         Block::Literal(ref mut l) => {
//                             let vec = std::mem::replace(l, Vec::new());
//                             self.state = Some(RestoreState::WriteVec {
//                                 write: write_all(w, vec),
//                                 r,
//                                 buf,
//                             })
//                         }
//                     }
//                 }
//                 Some(RestoreState::WriteBuf { mut write, r }) => match write.poll()? {
//                     Async::Ready((w, buf)) => {
//                         self.delta_pos = self.delta_pos + 1;
//                         self.state = Some(RestoreState::Delta { w, r, buf })
//                     }
//                     Async::NotReady => {
//                         self.state = Some(RestoreState::WriteBuf { write, r });
//                         return Ok(Async::NotReady);
//                     }
//                 },
//                 Some(RestoreState::WriteVec { mut write, r, buf }) => match write.poll()? {
//                     Async::Ready((w, vec)) => {
//                         self.delta.blocks[self.delta_pos] = Block::Literal(vec);
//                         self.delta_pos += 1;
//                         self.state = Some(RestoreState::Delta { w, r, buf })
//                     }
//                     Async::NotReady => {
//                         self.state = Some(RestoreState::WriteVec { write, r, buf });
//                         return Ok(Async::NotReady);
//                     }
//                 },
//                 None => panic!(""),
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use rand;
//     use super::*;
//     use rand::Rng;
//     use tokio_core::reactor::Core;
//     const WINDOW: usize = 32;
//     #[test]
//     fn basic() {
//         for index in 0..10 {
//             let source = rand::thread_rng()
//                 .gen_ascii_chars()
//                 .take(WINDOW * 10 + 8)
//                 .collect::<String>();
//             let mut modified = source.clone();
//             let index = WINDOW * index + 3;
//             unsafe {
//                 modified.as_bytes_mut()[index] =
//                     ((source.as_bytes()[index] as usize + 1) & 255) as u8
//             }
//             let block = [0; WINDOW];
//             let source_sig = signature(source.as_bytes(), block).unwrap();
//             let comp = compare(&source_sig, modified.as_bytes(), block).unwrap();

//             let mut restored = Vec::new();
//             let source = std::io::Cursor::new(source.as_bytes());
//             restore_seek(&mut restored, source, [0; WINDOW], &comp).unwrap();
//             if &restored[..] != modified.as_bytes() {
//                 for i in 0..10 {
//                     let a = &restored[i * WINDOW..(i + 1) * WINDOW];
//                     let b = &modified.as_bytes()[i * WINDOW..(i + 1) * WINDOW];
//                     // println!("{:?}\n{:?}\n", a, b);
//                     if a != b {
//                         // println!(">>>>>>>>");
//                     }
//                 }
//                 panic!("different");
//             }
//         }
//     }
//     #[test]
//     fn futures() {
//         let source = rand::thread_rng()
//             .gen_ascii_chars()
//             .take(WINDOW * 10 + 8)
//             .collect::<String>();
//         let mut modified = source.clone();
//         let index = WINDOW + 3;
//         unsafe {
//             modified.as_bytes_mut()[index] = ((source.as_bytes()[index] as usize + 1) & 255) as u8
//         }

//         let mut l = Core::new().unwrap();
//         let block = [0; WINDOW];
//         let source_sig = signature(source.as_bytes(), block).unwrap();
//         // println!("==================\n");
//         let (_, source_sig_) = l.run(signature_fut(source.as_bytes(), block)).unwrap();
//         assert_eq!(source_sig, source_sig_);
//         // println!("{:?} {:?}", source_sig, source_sig_);

//         let comp = compare(&source_sig, modified.as_bytes(), block).unwrap();
//         let (_, _, comp_) = l.run(compare_fut(source_sig, modified.as_bytes(), block)).unwrap();
//         assert_eq!(comp, comp_);
//         // println!("{:?}", comp);

//         let v = Vec::new();
//         let (rest_, _, _) = l.run(restore_seek_fut(
//             std::io::Cursor::new(v),
//             std::io::Cursor::new(source.as_bytes()),
//             [0; WINDOW],
//             comp,
//         )).unwrap();
//         assert_eq!(rest_.into_inner().as_slice(), modified.as_bytes());
//     }
// }
