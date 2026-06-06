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
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::*;

#[test]
#[should_panic(expected = "a keyed fair semaphore requires at least one permit")]
fn no_permits() {
    KeyedFairSemaphore::<usize>::new(0);
}

#[test]
fn try_acquire() {
    let semaphore = KeyedFairSemaphore::new(1);
    let permit = semaphore.try_acquire("a").unwrap();

    assert_eq!(permit.key(), &"a");
    assert_eq!(semaphore.available_permits(), 0);
    assert_eq!(semaphore.in_flight_for(&"a"), 1);
    assert!(semaphore.try_acquire("b").is_none());

    drop(permit);
    assert_eq!(semaphore.available_permits(), 1);
    assert!(semaphore.is_idle());
}

#[test]
fn balances_keys_with_fewer_in_flight_permits() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let semaphore = KeyedFairSemaphore::new(2);
    let permit_a0 = semaphore.try_acquire("a").unwrap();
    let permit_a1 = semaphore.try_acquire("a").unwrap();

    let waiter_a = semaphore.acquire("a");
    let mut waiter_a = pin!(waiter_a);
    assert!(waiter_a.as_mut().poll(&mut context).is_pending());

    let waiter_b = semaphore.acquire("b");
    let mut waiter_b = pin!(waiter_b);
    assert!(waiter_b.as_mut().poll(&mut context).is_pending());
    assert_eq!(semaphore.queued_waiters(), 2);

    drop(permit_a0);
    assert!(waiter_a.as_mut().poll(&mut context).is_pending());
    let permit_b = match waiter_b.as_mut().poll(&mut context) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("waiter for key b should be granted first"),
    };
    assert_eq!(permit_b.key(), &"b");

    drop(permit_a1);
    let permit_a = match waiter_a.as_mut().poll(&mut context) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("waiter for key a should be granted second"),
    };
    assert_eq!(permit_a.key(), &"a");
}

#[test]
fn preserves_fifo_within_key() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let semaphore = KeyedFairSemaphore::new(1);
    let held = semaphore.try_acquire(7usize).unwrap();

    let waiter1 = semaphore.acquire(7usize);
    let mut waiter1 = pin!(waiter1);
    assert!(waiter1.as_mut().poll(&mut context).is_pending());

    let waiter2 = semaphore.acquire(7usize);
    let mut waiter2 = pin!(waiter2);
    assert!(waiter2.as_mut().poll(&mut context).is_pending());

    drop(held);
    let permit1 = match waiter1.as_mut().poll(&mut context) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("first waiter should be granted first"),
    };
    assert!(waiter2.as_mut().poll(&mut context).is_pending());

    drop(permit1);
    let permit2 = match waiter2.as_mut().poll(&mut context) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("second waiter should be granted second"),
    };
    assert_eq!(permit2.key(), &7);
}

#[test]
fn cancelling_waiter_removes_it_from_queue() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let semaphore = KeyedFairSemaphore::new(1);
    let held = semaphore.try_acquire(1usize).unwrap();

    {
        let waiter = semaphore.acquire(2usize);
        let mut waiter = pin!(waiter);
        assert!(waiter.as_mut().poll(&mut context).is_pending());
        assert_eq!(semaphore.queued_waiters(), 1);
    }

    assert_eq!(semaphore.queued_waiters(), 0);
    drop(held);
    assert_eq!(semaphore.available_permits(), 1);

    let permit = semaphore.try_acquire(3usize).unwrap();
    assert_eq!(permit.key(), &3);
}

#[test]
fn cancelling_granted_waiter_releases_permit() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let semaphore = KeyedFairSemaphore::new(1);
    let held = semaphore.try_acquire(1usize).unwrap();

    {
        let waiter = semaphore.acquire(2usize);
        let mut waiter = pin!(waiter);
        assert!(waiter.as_mut().poll(&mut context).is_pending());

        drop(held);
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(semaphore.queued_waiters(), 0);
        assert_eq!(semaphore.in_flight_for(&2), 1);
    }

    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(semaphore.in_flight_for(&2), 0);

    let permit = semaphore.try_acquire(3usize).unwrap();
    assert_eq!(permit.key(), &3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_test_preserves_capacity() {
    let semaphore = Arc::new(KeyedFairSemaphore::new(3));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for key in 0..5usize {
        for _ in 0..32usize {
            let semaphore = semaphore.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned(key).await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(max_active.load(Ordering::SeqCst) <= 3);
    assert_eq!(semaphore.available_permits(), 3);
    assert_eq!(semaphore.queued_waiters(), 0);
    assert!(semaphore.is_idle());
}
