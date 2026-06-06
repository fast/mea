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

//! A keyed semaphore that schedules waiters fairly across keys.
//!
//! A [`KeyedFairSemaphore`] maintains a fixed set of permits. Each acquire
//! operation belongs to a key, and the semaphore prefers pending waiters whose
//! key has the fewest in-flight permits. Ties are resolved by queue order, so
//! waiters with the same in-flight count are served first-in, first-out.
//!
//! This is useful when many tasks compete for the same bounded resource and
//! each task belongs to a key, such as a tenant, statement, partition, or work
//! queue. Compared with a plain FIFO semaphore, it prevents one busy key from
//! monopolizing all released permits while other keys are waiting.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use mea::keyed_fair_semaphore::KeyedFairSemaphore;
//!
//! let semaphore = Arc::new(KeyedFairSemaphore::new(2));
//! let permit_a = semaphore.clone().acquire_owned("a").await;
//! let permit_b = semaphore.clone().acquire_owned("b").await;
//!
//! assert_eq!(semaphore.available_permits(), 0);
//! assert_eq!(permit_a.key(), &"a");
//! assert_eq!(permit_b.key(), &"b");
//! # }
//! ```

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::Mutex;

#[cfg(test)]
mod tests;

/// A keyed semaphore that schedules waiters fairly across keys.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct KeyedFairSemaphore<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    state: Mutex<KeyedFairSemaphoreState<K, S>>,
}

impl<K> KeyedFairSemaphore<K, RandomState>
where
    K: Eq + Hash,
{
    /// Creates a new keyed fair semaphore with the given number of permits.
    ///
    /// # Panics
    ///
    /// Panics if `permits` is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use mea::keyed_fair_semaphore::KeyedFairSemaphore;
    ///
    /// let semaphore = KeyedFairSemaphore::<String>::new(3);
    /// assert_eq!(semaphore.total_permits(), 3);
    /// assert_eq!(semaphore.available_permits(), 3);
    /// ```
    pub fn new(permits: usize) -> Self {
        Self::with_hasher(permits, RandomState::new())
    }
}

impl<K, S> KeyedFairSemaphore<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Creates a new keyed fair semaphore with the given number of permits and hasher.
    ///
    /// # Panics
    ///
    /// Panics if `permits` is zero.
    pub fn with_hasher(permits: usize, hasher: S) -> Self {
        assert!(
            permits > 0,
            "a keyed fair semaphore requires at least one permit"
        );
        Self {
            state: Mutex::new(KeyedFairSemaphoreState::new(permits, hasher)),
        }
    }

    /// Returns the total number of permits managed by this semaphore.
    pub fn total_permits(&self) -> usize {
        self.state.lock().total_permits
    }

    /// Returns the current number of available permits.
    ///
    /// Granted waiters that have not yet been polled again already count as
    /// in-flight, so they are not included in this number.
    pub fn available_permits(&self) -> usize {
        self.state.lock().available_permits
    }

    /// Returns the number of pending waiters.
    ///
    /// Waiters that have already been granted a permit but have not yet been
    /// polled again are not counted.
    pub fn queued_waiters(&self) -> usize {
        self.state.lock().pending_waiters()
    }

    /// Returns the number of in-flight permits currently held by `key`.
    pub fn in_flight_for(&self, key: &K) -> usize {
        self.state.lock().in_flight_for(key)
    }

    /// Returns `true` when all permits are available and no waiters remain.
    pub fn is_idle(&self) -> bool {
        let state = self.state.lock();
        state.available_permits == state.total_permits
            && state.in_flight_by_key.is_empty()
            && state.pending_waiters() == 0
    }

    /// Attempts to acquire one permit for `key` without waiting.
    ///
    /// This method succeeds only when a permit is available and there are no
    /// pending waiters. If another key is already waiting, new callers must
    /// join the queue so the keyed fair scheduling rule is preserved.
    pub fn try_acquire(&self, key: K) -> Option<KeyedFairSemaphorePermit<'_, K, S>> {
        let key = Arc::new(key);
        let acquired = {
            let mut state = self.state.lock();
            state.try_acquire_now(key.clone())
        };

        acquired.then_some(KeyedFairSemaphorePermit {
            semaphore: self,
            key,
        })
    }

    /// Acquires one permit for `key`.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute permits across keys.
    /// Cancelling a call to `acquire` makes you lose your place in the queue.
    /// If the waiter has already been granted a permit but the future is
    /// cancelled before returning, the permit is released.
    pub async fn acquire(&self, key: K) -> KeyedFairSemaphorePermit<'_, K, S> {
        let key = Arc::new(key);
        Acquire::new(self, key.clone()).await;
        KeyedFairSemaphorePermit {
            semaphore: self,
            key,
        }
    }

    /// Attempts to acquire one owned permit for `key` without waiting.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    pub fn try_acquire_owned(
        self: Arc<Self>,
        key: K,
    ) -> Option<OwnedKeyedFairSemaphorePermit<K, S>> {
        let key = Arc::new(key);
        let acquired = {
            let mut state = self.state.lock();
            state.try_acquire_now(key.clone())
        };

        acquired.then_some(OwnedKeyedFairSemaphorePermit {
            semaphore: self,
            key,
        })
    }

    /// Acquires one owned permit for `key`.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    ///
    /// # Cancel safety
    ///
    /// This method has the same cancel-safety behavior as [`Self::acquire`].
    pub async fn acquire_owned(self: Arc<Self>, key: K) -> OwnedKeyedFairSemaphorePermit<K, S> {
        let key = Arc::new(key);
        Acquire::new(&self, key.clone()).await;
        OwnedKeyedFairSemaphorePermit {
            semaphore: self,
            key,
        }
    }

    fn release(&self, key: &Arc<K>) {
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            state.release(key);
            state.grant_waiters(&mut wakers);
        }
        wake_all(wakers);
    }
}

#[derive(Debug)]
struct KeyedFairSemaphoreState<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    total_permits: usize,
    available_permits: usize,
    next_waiter_id: u64,
    in_flight_by_key: HashMap<Arc<K>, usize, S>,
    waiters: VecDeque<Waiter<K>>,
}

impl<K, S> KeyedFairSemaphoreState<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(permits: usize, hasher: S) -> Self {
        Self {
            total_permits: permits,
            available_permits: permits,
            next_waiter_id: 0,
            in_flight_by_key: HashMap::with_hasher(hasher),
            waiters: VecDeque::new(),
        }
    }

    fn pending_waiters(&self) -> usize {
        self.waiters.iter().filter(|waiter| !waiter.granted).count()
    }

    fn has_pending_waiters(&self) -> bool {
        self.waiters.iter().any(|waiter| !waiter.granted)
    }

    fn in_flight_for(&self, key: &K) -> usize {
        self.in_flight_by_key.get(key).copied().unwrap_or_default()
    }

    fn try_acquire_now(&mut self, key: Arc<K>) -> bool {
        if self.available_permits == 0 || self.has_pending_waiters() {
            return false;
        }

        self.available_permits -= 1;
        self.increment_in_flight(key);
        true
    }

    fn push_waiter(&mut self, key: Arc<K>, waker: &Waker) -> u64 {
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id = self
            .next_waiter_id
            .checked_add(1)
            .expect("number of keyed fair semaphore waiters would overflow u64::MAX");

        self.waiters.push_back(Waiter {
            id: waiter_id,
            key,
            waker: Some(waker.clone()),
            granted: false,
        });

        waiter_id
    }

    fn poll_waiter(&mut self, waiter_id: u64, waker: &Waker) -> Poll<()> {
        let Some(index) = self.waiter_index(waiter_id) else {
            panic!("keyed fair semaphore waiter is missing");
        };

        let waiter = &mut self.waiters[index];
        if waiter.granted {
            self.waiters.remove(index);
            Poll::Ready(())
        } else {
            let update_waker = waiter
                .waker
                .as_ref()
                .is_none_or(|current| !current.will_wake(waker));
            if update_waker {
                waiter.waker = Some(waker.clone());
            }
            Poll::Pending
        }
    }

    fn remove_waiter(&mut self, waiter_id: u64) -> Option<Waiter<K>> {
        let index = self.waiter_index(waiter_id)?;
        self.waiters.remove(index)
    }

    fn grant_waiters(&mut self, wakers: &mut Vec<Waker>) {
        while self.available_permits > 0 {
            let Some(index) = self.next_waiter_to_grant() else {
                return;
            };

            self.available_permits -= 1;
            let (key, waker) = {
                let waiter = &mut self.waiters[index];
                waiter.granted = true;
                (waiter.key.clone(), waiter.waker.take())
            };
            self.increment_in_flight(key);

            if let Some(waker) = waker {
                wakers.push(waker);
            }
        }
    }

    fn next_waiter_to_grant(&self) -> Option<usize> {
        let mut best = None;
        let mut best_in_flight = usize::MAX;

        for (index, waiter) in self.waiters.iter().enumerate() {
            if waiter.granted {
                continue;
            }

            let in_flight = self
                .in_flight_by_key
                .get(&waiter.key)
                .copied()
                .unwrap_or_default();
            if in_flight < best_in_flight {
                best = Some(index);
                best_in_flight = in_flight;
            }
        }

        best
    }

    fn release(&mut self, key: &Arc<K>) {
        let Some(in_flight) = self.in_flight_by_key.get_mut(key) else {
            return;
        };

        *in_flight -= 1;
        if *in_flight == 0 {
            self.in_flight_by_key.remove(key);
        }

        self.available_permits += 1;
        debug_assert!(self.available_permits <= self.total_permits);
    }

    fn increment_in_flight(&mut self, key: Arc<K>) {
        *self.in_flight_by_key.entry(key).or_default() += 1;
    }

    fn waiter_index(&self, waiter_id: u64) -> Option<usize> {
        self.waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
    }
}

#[derive(Debug)]
struct Waiter<K>
where
    K: Eq + Hash,
{
    id: u64,
    key: Arc<K>,
    waker: Option<Waker>,
    granted: bool,
}

#[derive(Debug)]
struct Acquire<'a, K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    semaphore: &'a KeyedFairSemaphore<K, S>,
    key: Arc<K>,
    waiter_id: Option<u64>,
    done: bool,
}

impl<'a, K, S> Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(semaphore: &'a KeyedFairSemaphore<K, S>, key: Arc<K>) -> Self {
        Self {
            semaphore,
            key,
            waiter_id: None,
            done: false,
        }
    }
}

impl<K, S> Drop for Acquire<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };

        let mut wakers = Vec::new();
        {
            let mut state = self.semaphore.state.lock();
            if let Some(waiter) = state.remove_waiter(waiter_id)
                && waiter.granted
            {
                state.release(&waiter.key);
                state.grant_waiters(&mut wakers);
            }
        }
        wake_all(wakers);
    }
}

impl<K, S> Future for Acquire<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(());
        }

        if let Some(waiter_id) = this.waiter_id {
            let mut state = this.semaphore.state.lock();
            if state.poll_waiter(waiter_id, cx.waker()).is_ready() {
                this.waiter_id = None;
                this.done = true;
                return Poll::Ready(());
            }
            return Poll::Pending;
        }

        let mut wakers = Vec::new();
        let mut ready = false;
        {
            let mut state = this.semaphore.state.lock();
            if state.try_acquire_now(this.key.clone()) {
                this.done = true;
                return Poll::Ready(());
            }

            let waiter_id = state.push_waiter(this.key.clone(), cx.waker());
            this.waiter_id = Some(waiter_id);
            state.grant_waiters(&mut wakers);
            if state.poll_waiter(waiter_id, cx.waker()).is_ready() {
                this.waiter_id = None;
                this.done = true;
                ready = true;
            }
        }
        wake_all(wakers);

        if ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// A permit acquired from a [`KeyedFairSemaphore`].
///
/// When the permit is dropped, it is released back to the semaphore.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct KeyedFairSemaphorePermit<'a, K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    semaphore: &'a KeyedFairSemaphore<K, S>,
    key: Arc<K>,
}

impl<K, S> KeyedFairSemaphorePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key that this permit belongs to.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, S> Drop for KeyedFairSemaphorePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.semaphore.release(&self.key);
    }
}

/// An owned permit acquired from an [`Arc<KeyedFairSemaphore>`].
///
/// When the permit is dropped, it is released back to the semaphore.
///
/// [`Arc<KeyedFairSemaphore>`]: std::sync::Arc
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedKeyedFairSemaphorePermit<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    semaphore: Arc<KeyedFairSemaphore<K, S>>,
    key: Arc<K>,
}

impl<K, S> OwnedKeyedFairSemaphorePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key that this permit belongs to.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, S> Drop for OwnedKeyedFairSemaphorePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.semaphore.release(&self.key);
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}
