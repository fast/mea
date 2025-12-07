// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A multi-producer multi-consumer broadcast channel.
//!
//! This channel supports multiple senders and multiple receivers. Each message sent by any
//! sender is received by all receivers. If a receiver falls behind, it may miss messages,
//! which is reported via [`RecvError::Lagged`].
//!
//! # Examples
//!
//! ```
//! use mea::broadcast;
//!
//! #[tokio::main]
//! async fn main() {
//!     let (tx, mut rx1) = broadcast::channel(16);
//!     let mut rx2 = rx1.clone();
//!
//!     tx.send(10);
//!     tx.send(20);
//!
//!     assert_eq!(rx1.recv().await, Ok(10));
//!     assert_eq!(rx2.recv().await, Ok(10));
//!     assert_eq!(rx1.recv().await, Ok(20));
//!     assert_eq!(rx2.recv().await, Ok(20));
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::Mutex;
use crate::internal::RwLock;
use crate::internal::WaitSet;

/// Error returned by [`Receiver::recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// The receiver lagged too far behind.
    ///
    /// The count is the number of messages skipped. The receiver's internal cursor has been
    /// advanced to the oldest available message.
    Lagged(u64),
    /// The channel has been closed.
    Closed,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Lagged(n) => write!(f, "receiver lagged by {n}"),
            RecvError::Closed => write!(f, "channel closed"),
        }
    }
}

impl std::error::Error for RecvError {}

#[derive(Debug)]
struct Slot<T> {
    /// The message. None if the slot is empty (only initially).
    msg: Option<T>,
    /// The absolute version of the message in this slot.
    version: u64,
}

struct Shared<T> {
    buffer: Box<[RwLock<Slot<T>>]>,
    capacity: usize,
    /// The global tail cursor. Points to the next slot to write.
    /// Strictly monotonically increasing.
    tail_cnt: AtomicU64,
    /// Number of active senders.
    senders: AtomicUsize,
    /// Waiters (receivers) waiting for new messages.
    waiters: Mutex<WaitSet>,
}

/// A sender handle to the broadcast channel.
///
/// The sender can be cloned to create multiple producers. When all senders are dropped,
/// the channel is closed.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Wake all receivers so they can see the Closed state.
            // We use the waiters lock.
            self.shared.waiters.lock().wake_all();
        }
    }
}

impl<T> Sender<T> {
    /// Broadcasts a value to all active receivers.
    ///
    /// This operation is non-blocking. If the channel buffer is full, the oldest message
    /// in the buffer is overwritten. Any receiver that was waiting for that overwritten
    /// message will receive a [`RecvError::Lagged`] error on its next call to `recv`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mea::broadcast;
    ///
    /// let (tx, _) = broadcast::channel(10);
    /// tx.send(42);
    /// ```
    pub fn send(&self, msg: T) {
        let tail = self.shared.tail_cnt.fetch_add(1, Ordering::SeqCst);
        let cap = self.shared.capacity;
        let idx = (tail % cap as u64) as usize;

        {
            let mut slot = self.shared.buffer[idx].write();
            slot.msg = Some(msg);
            slot.version = tail;
        }

        // Notify all waiting receivers
        self.shared.waiters.lock().wake_all();
    }

    /// Creates a new receiver that starts receiving messages from the current tail of the channel.
    pub fn subscribe(&self) -> Receiver<T> {
        let tail = self.shared.tail_cnt.load(Ordering::SeqCst);
        Receiver {
            shared: self.shared.clone(),
            head: tail,
            waker_key: None,
        }
    }

    /// Returns the number of active senders.
    pub fn sender_count(&self) -> usize {
        self.shared.senders.load(Ordering::SeqCst)
    }

    /// Returns the number of messages in the channel.
    ///
    /// This is an estimate as the channel is concurrent.
    pub fn len(&self) -> usize {
        let tail = self.shared.tail_cnt.load(Ordering::Relaxed);
        let cap = self.shared.capacity as u64;
        if tail < cap {
            tail as usize
        } else {
            self.shared.capacity
        }
    }

    /// Returns `true` if the channel is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A receiver handle to the broadcast channel.
///
/// The receiver can be cloned to create multiple consumers. Each receiver sees every
/// message sent to the channel (unless it lags behind).
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The next message to read.
    head: u64,
    /// Key for the WaitSet to identify this receiver's waker.
    waker_key: Option<usize>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            head: self.head,
            waker_key: None,
        }
    }
}

impl<T: Clone> Receiver<T> {
    /// Receives the next value for this receiver.
    ///
    /// # Return Value
    ///
    /// - `Ok(T)`: The next message.
    /// - `Err(RecvError::Lagged(u64))`: The receiver lagged behind. The internal cursor is advanced
    ///   to the oldest available message. The count indicates how many messages were skipped.
    /// - `Err(RecvError::Closed)`: All senders have been dropped and no more messages are
    ///   available.
    ///
    /// # Examples
    ///
    /// Handling lag:
    ///
    /// ```
    /// use mea::broadcast;
    /// use mea::broadcast::RecvError;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = broadcast::channel(2);
    ///
    ///     tx.send(1);
    ///     tx.send(2);
    ///     tx.send(3); // Overwrites 1
    ///
    ///     assert_eq!(rx.recv().await, Err(RecvError::Lagged(1)));
    ///     assert_eq!(rx.recv().await, Ok(2));
    ///     assert_eq!(rx.recv().await, Ok(3));
    /// }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        RecvFuture { receiver: self }.await
    }
}

impl<T> Receiver<T> {
    /// Re-subscribes to the channel, returning a new receiver that starts receiving messages
    /// from the *current* tail of the channel.
    ///
    /// This is useful if the receiver has lagged too far behind and wants to jump to the latest
    /// message, skipping everything in between.
    pub fn resubscribe(&self) -> Self {
        let tail = self.shared.tail_cnt.load(Ordering::SeqCst);
        Self {
            shared: self.shared.clone(),
            head: tail,
            waker_key: None,
        }
    }
}

struct RecvFuture<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T: Clone> Future for RecvFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = &mut self.receiver;
        let shared = &receiver.shared;
        let cap = shared.capacity;

        loop {
            let tail = shared.tail_cnt.load(Ordering::SeqCst);
            let head = receiver.head;

            // 1. Check for Lag
            if tail > head && (tail - head) > cap as u64 {
                let oldest_valid = tail - cap as u64;
                let missed = oldest_valid - head;
                receiver.head = oldest_valid;
                return Poll::Ready(Err(RecvError::Lagged(missed)));
            }

            // 2. Check if we have a message
            if head < tail {
                let idx = (head % cap as u64) as usize;

                // Read lock the slot
                let slot = shared.buffer[idx].read();

                if slot.version == head {
                    if let Some(msg) = &slot.msg {
                        receiver.head += 1;
                        return Poll::Ready(Ok(msg.clone()));
                    }
                }

                // If version != head, it means the slot was overwritten (lagged).
                // Or if msg is None (should not happen if head < tail, unless wrapped around and
                // version mismatched). In any case of mismatch, we loop back. The
                // Lag check at the top should catch the lag state in the next
                // iteration because tail has advanced.
                drop(slot);
                continue;
            }

            // 3. No message available (head == tail), prepare to wait.
            let mut waiters = shared.waiters.lock();

            // Double check tail to avoid race condition where message is sent
            // after we checked tail but before we locked waiters.
            let tail_now = shared.tail_cnt.load(Ordering::SeqCst);
            if tail_now > head {
                // New message arrived!
                drop(waiters);
                continue;
            }

            // 4. Check for Closed
            if shared.senders.load(Ordering::SeqCst) == 0 {
                return Poll::Ready(Err(RecvError::Closed));
            }

            // 5. Register Waker
            waiters.register_waker(&mut receiver.waker_key, cx);
            return Poll::Pending;
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // WaitSet cleans up lazily.
    }
}

/// Creates a new broadcast channel with the given capacity.
///
/// The channel implements a "tail drop" policy: if the buffer is full, new messages
/// overwrite the oldest ones.
///
/// # Panics
///
/// Panics if `capacity` is 0.
///
/// # Examples
///
/// ```
/// use mea::broadcast;
///
/// let (tx, rx) = broadcast::channel::<i32>(32);
/// ```
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be greater than 0");

    let mut buffer = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        buffer.push(RwLock::new(Slot {
            msg: None,
            version: 0,
        }));
    }

    let shared = Arc::new(Shared {
        buffer: buffer.into_boxed_slice(),
        capacity,
        tail_cnt: AtomicU64::new(0),
        senders: AtomicUsize::new(1),
        waiters: Mutex::new(WaitSet::new()),
    });

    let sender = Sender {
        shared: shared.clone(),
    };

    let receiver = Receiver {
        shared,
        head: 0,
        waker_key: None,
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests;
