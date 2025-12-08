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

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

//! # Mea - Make Easy Async
//!
//! `mea` is a runtime-agnostic library providing essential synchronization primitives for
//! asynchronous Rust programming. The library offers a collection of well-tested, efficient
//! synchronization tools that work with any async runtime.
//!
//! ## Features
//!
//! * [`Barrier`]: A synchronization point where multiple tasks can wait until all participants
//!   arrive
//! * [`Condvar`]: A condition variable that allows tasks to wait for a notification
//! * [`Latch`]: A single-use barrier that allows one or more tasks to wait until a signal is given
//! * [`Mutex`]: A mutual exclusion primitive for protecting shared data
//! * [`Once`]: A primitive that ensures a one-time asynchronous operation runs at most once, even
//!   when called concurrently
//! * [`OnceCell`]: A cell that can be written to at most once and provides safe concurrent access
//! * [`RwLock`]: A reader-writer lock that allows multiple readers or a single writer at a time
//! * [`Semaphore`]: A synchronization primitive that controls access to a shared resource
//! * [`ShutdownSend`] & [`ShutdownRecv`]: A composite synchronization primitive for managing
//!   shutdown signals
//! * [`WaitGroup`]: A synchronization primitive that allows waiting for multiple tasks to complete
//! * [`atomicbox`]: A safe, owning version of `AtomicPtr` for heap-allocated data.
//! * [`broadcast::channel`]: A multi-producer, multi-consumer broadcast channel.
//! * [`mpsc::bounded`]: A multi-producer, single-consumer bounded queue for sending values between
//!   asynchronous tasks.
//! * [`mpsc::unbounded`]: A multi-producer, single-consumer unbounded queue for sending values
//!   between asynchronous tasks.
//! * [`oneshot::channel`]: A one-shot channel for sending a single value between tasks.
//!
//! ## Runtime Agnostic
//!
//! All synchronization primitives in this library are runtime-agnostic, meaning they can be used
//! with any async runtime like Tokio, async-std, or others. This makes the library highly versatile
//! and portable.
//!
//! ## Thread Safety
//!
//! All types in this library implement `Send` and `Sync`, making them safe to share across thread
//! boundaries. This is essential for concurrent programming where data needs to be accessed from
//! multiple threads.
//!
//! [`Barrier`]: barrier::Barrier
//! [`Condvar`]: condvar::Condvar
//! [`Latch`]: latch::Latch
//! [`Mutex`]: mutex::Mutex
//! [`Once`]: once::Once
//! [`OnceCell`]: once::OnceCell
//! [`RwLock`]: rwlock::RwLock
//! [`Semaphore`]: semaphore::Semaphore
//! [`ShutdownSend`]: shutdown::ShutdownSend
//! [`ShutdownRecv`]: shutdown::ShutdownRecv
//! [`WaitGroup`]: waitgroup::WaitGroup

mod internal;

pub mod atomicbox;
pub mod barrier;
pub mod broadcast;
pub mod condvar;
pub mod latch;
pub mod mpsc;
pub mod mutex;
pub mod once;
pub mod oneshot;
pub mod rwlock;
pub mod semaphore;
pub mod shutdown;
pub mod waitgroup;

#[cfg(test)]
fn test_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;

    use tokio::runtime::Runtime;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

#[cfg(test)]
mod tests {
    use crate::barrier::Barrier;
    use crate::broadcast;
    use crate::condvar::Condvar;
    use crate::latch::Latch;
    use crate::mpsc;
    use crate::mutex::Mutex;
    use crate::mutex::MutexGuard;
    use crate::once::Once;
    use crate::once::OnceCell;
    use crate::oneshot;
    use crate::rwlock::RwLock;
    use crate::rwlock::RwLockReadGuard;
    use crate::rwlock::RwLockWriteGuard;
    use crate::semaphore::Semaphore;
    use crate::shutdown::ShutdownRecv;
    use crate::shutdown::ShutdownSend;
    use crate::waitgroup::Wait;
    use crate::waitgroup::WaitGroup;

    #[test]
    fn assert_send_and_sync() {
        fn do_assert_send_and_sync<T: Send + Sync>() {}
        do_assert_send_and_sync::<Barrier>();
        do_assert_send_and_sync::<Condvar>();
        do_assert_send_and_sync::<Once>();
        do_assert_send_and_sync::<OnceCell<u32>>();
        do_assert_send_and_sync::<Latch>();
        do_assert_send_and_sync::<Semaphore>();
        do_assert_send_and_sync::<ShutdownSend>();
        do_assert_send_and_sync::<ShutdownRecv>();
        do_assert_send_and_sync::<WaitGroup>();
        do_assert_send_and_sync::<Mutex<i64>>();
        do_assert_send_and_sync::<MutexGuard<'_, i64>>();
        do_assert_send_and_sync::<RwLock<i64>>();
        do_assert_send_and_sync::<RwLockReadGuard<'_, i64>>();
        do_assert_send_and_sync::<RwLockWriteGuard<'_, i64>>();
        do_assert_send_and_sync::<broadcast::Sender<i64>>();
        do_assert_send_and_sync::<broadcast::Receiver<i64>>();
        do_assert_send_and_sync::<broadcast::RecvError>();
        do_assert_send_and_sync::<broadcast::TryRecvError>();
        do_assert_send_and_sync::<oneshot::SendError<i64>>();
        do_assert_send_and_sync::<oneshot::Sender<i64>>();
        do_assert_send_and_sync::<mpsc::SendError<i64>>();
        do_assert_send_and_sync::<mpsc::UnboundedSender<i64>>();
        do_assert_send_and_sync::<mpsc::UnboundedReceiver<i64>>();
        do_assert_send_and_sync::<mpsc::BoundedSender<i64>>();
        do_assert_send_and_sync::<mpsc::BoundedReceiver<i64>>();
    }

    #[test]
    fn assert_send() {
        fn do_assert_send<T: Send>() {}
        do_assert_send::<oneshot::Receiver<i64>>();
        do_assert_send::<oneshot::Recv<i64>>();
    }

    #[test]
    fn assert_unpin() {
        fn do_assert_unpin<T: Unpin>() {}
        do_assert_unpin::<Barrier>();
        do_assert_unpin::<Condvar>();
        do_assert_unpin::<Latch>();
        do_assert_unpin::<Once>();
        do_assert_unpin::<OnceCell<u32>>();
        do_assert_unpin::<Semaphore>();
        do_assert_unpin::<ShutdownSend>();
        do_assert_unpin::<ShutdownRecv>();
        do_assert_unpin::<WaitGroup>();
        do_assert_unpin::<Wait>();
        do_assert_unpin::<Mutex<i64>>();
        do_assert_unpin::<MutexGuard<'_, i64>>();
        do_assert_unpin::<RwLock<i64>>();
        do_assert_unpin::<RwLockReadGuard<'_, i64>>();
        do_assert_unpin::<RwLockWriteGuard<'_, i64>>();
        do_assert_unpin::<broadcast::Sender<i64>>();
        do_assert_unpin::<broadcast::Receiver<i64>>();
        do_assert_unpin::<broadcast::RecvError>();
        do_assert_unpin::<broadcast::TryRecvError>();
        do_assert_unpin::<oneshot::Sender<i64>>();
        do_assert_unpin::<oneshot::SendError<i64>>();
        do_assert_unpin::<oneshot::Receiver<i64>>();
        do_assert_unpin::<oneshot::Recv<i64>>();
        do_assert_unpin::<mpsc::SendError<i64>>();
        do_assert_unpin::<mpsc::UnboundedSender<i64>>();
        do_assert_unpin::<mpsc::UnboundedReceiver<i64>>();
        do_assert_unpin::<mpsc::BoundedSender<i64>>();
        do_assert_unpin::<mpsc::BoundedReceiver<i64>>();
    }
}
