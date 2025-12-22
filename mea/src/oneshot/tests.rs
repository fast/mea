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

use std::future::Future;
use std::future::IntoFuture;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::time::Duration;

use crate::oneshot;
use crate::oneshot::TryRecvError;

struct DropCounterHandle(Arc<AtomicUsize>);

impl DropCounterHandle {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

struct DropCounter<T> {
    drop_count: Arc<AtomicUsize>,
    value: Option<T>,
}

impl<T> DropCounter<T> {
    fn new(value: T) -> (Self, DropCounterHandle) {
        let drop_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                drop_count: drop_count.clone(),
                value: Some(value),
            },
            DropCounterHandle(drop_count),
        )
    }

    fn value(&self) -> &T {
        self.value.as_ref().unwrap()
    }
}

impl<T> Drop for DropCounter<T> {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn send_before_await() {
    let (sender, receiver) = oneshot::channel();
    assert!(sender.send(19i128).is_ok());
    assert_eq!(receiver.await, Ok(19i128));
}

#[tokio::test]
async fn await_with_dropped_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    drop(sender);
    receiver.await.unwrap_err();
}

#[tokio::test]
async fn await_before_send() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(79u128);
    let t = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send(message)
    });
    let returned_message = receiver.await.unwrap();
    assert_eq!(counter.count(), 0);
    assert_eq!(*returned_message.value(), 79u128);
    drop(returned_message);
    assert_eq!(counter.count(), 1);
    t.await.unwrap().unwrap();
}

#[tokio::test]
async fn await_before_send_then_drop_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let t = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(sender);
    });
    assert!(receiver.await.is_err());
    t.await.unwrap();
}

#[tokio::test]
async fn poll_receiver_then_drop_it() {
    let (sender, receiver) = oneshot::channel::<()>();
    // This will poll the receiver and then give up after 100 ms.
    tokio::time::timeout(Duration::from_millis(100), receiver)
        .await
        .unwrap_err();
    // Make sure the receiver has been dropped by the runtime.
    assert!(sender.send(()).is_err());
}

#[tokio::test]
async fn recv_within_select() {
    let (tx, rx) = oneshot::channel::<&'static str>();
    let mut interval = tokio::time::interval(Duration::from_millis(10));

    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send("shut down").unwrap();
    });

    let mut recv = rx.into_future();
    loop {
        tokio::select! {
            _ = interval.tick() => println!("another 10ms"),
            msg = &mut recv => {
                println!("Got message: {}", msg.unwrap());
                break;
            }
        }
    }

    handle.await.unwrap();
}

#[test]
fn try_recv_success_then_disconnected() {
    let (tx, rx) = oneshot::channel::<i32>();
    tx.send(10).unwrap();

    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn try_recv_empty_with_live_sender() {
    let (_tx, rx) = oneshot::channel::<()>();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn try_recv_disconnected_after_drop() {
    let (tx, rx) = oneshot::channel::<()>();
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[derive(Default)]
pub struct WakerHandle {
    clone_count: AtomicU32,
    drop_count: AtomicU32,
    wake_count: AtomicU32,
}

impl WakerHandle {
    pub fn clone_count(&self) -> u32 {
        self.clone_count.load(Ordering::Relaxed)
    }

    pub fn drop_count(&self) -> u32 {
        self.drop_count.load(Ordering::Relaxed)
    }

    pub fn wake_count(&self) -> u32 {
        self.wake_count.load(Ordering::Relaxed)
    }
}

fn waker() -> (Waker, Arc<WakerHandle>) {
    let waker_handle = Arc::new(WakerHandle::default());
    let waker_handle_ptr = Arc::into_raw(waker_handle.clone());
    let raw_waker = RawWaker::new(waker_handle_ptr as *const _, waker_vtable());
    (unsafe { Waker::from_raw(raw_waker) }, waker_handle)
}

fn waker_vtable() -> &'static RawWakerVTable {
    &RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw)
}

unsafe fn clone_raw(data: *const ()) -> RawWaker {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.clone_count.fetch_add(1, Ordering::Relaxed);
    mem::forget(handle.clone());
    mem::forget(handle);
    RawWaker::new(data, waker_vtable())
}

unsafe fn wake_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.wake_count.fetch_add(1, Ordering::Relaxed);
    handle.drop_count.fetch_add(1, Ordering::Relaxed);
}

unsafe fn wake_by_ref_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.wake_count.fetch_add(1, Ordering::Relaxed);
    mem::forget(handle)
}

unsafe fn drop_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.drop_count.fetch_add(1, Ordering::Relaxed);
    drop(handle)
}

#[test]
fn poll_then_send() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 0);
    assert_eq!(waker_handle.wake_count(), 0);

    sender.send(1234).unwrap();
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);

    assert_eq!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Ready(Ok(1234))
    );
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);
}

#[test]
fn poll_with_different_wakers() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker1, waker_handle1) = waker();
    let mut context1 = Context::from_waker(&waker1);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context1), Poll::Pending);
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 0);
    assert_eq!(waker_handle1.wake_count(), 0);

    let (waker2, waker_handle2) = waker();
    let mut context2 = Context::from_waker(&waker2);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context2), Poll::Pending);
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 1);
    assert_eq!(waker_handle1.wake_count(), 0);

    assert_eq!(waker_handle2.clone_count(), 1);
    assert_eq!(waker_handle2.drop_count(), 0);
    assert_eq!(waker_handle2.wake_count(), 0);

    // Sending should cause the waker from the latest poll to be woken up
    sender.send(1234).unwrap();
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 1);
    assert_eq!(waker_handle1.wake_count(), 0);

    assert_eq!(waker_handle2.clone_count(), 1);
    assert_eq!(waker_handle2.drop_count(), 1);
    assert_eq!(waker_handle2.wake_count(), 1);
}

#[test]
fn poll_then_drop_receiver_during_send() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, _waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    // Put the channel into the receiving state
    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);

    // Spawn a separate thread that sends in parallel
    let t = std::thread::spawn(move || {
        let _ = sender.send(1234);
    });

    // Drop the receiver.
    drop(receiver);

    // The send operation should also not have panicked
    t.join().unwrap();
}
