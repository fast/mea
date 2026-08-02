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

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use slab::Slab;

use crate::internal::Mutex;

#[derive(Debug)]
pub(super) struct Share<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    state: Mutex<State<K, S>>,
}

impl<K, S> Share<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(super) fn new(admission_limits: Box<[usize]>, hash_builder: S) -> Self {
        Self {
            state: Mutex::new(State::new(admission_limits, hash_builder)),
        }
    }

    pub(super) fn available_permits(&self) -> usize {
        self.state.lock().available_permits
    }

    pub(super) fn num_waiters(&self) -> usize {
        self.state.lock().num_waiters
    }

    pub(super) fn try_acquire(&self, key: Arc<K>, priority: usize) -> bool {
        self.state.lock().try_admit(key, priority)
    }

    pub(super) fn acquire(&self, key: Arc<K>, priority: usize) -> Acquire<'_, K, S> {
        Acquire::new(self, key, priority)
    }

    pub(super) fn release(&self, key: &K) {
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            state.release(key);
            state.admit_waiters(&mut wakers);
        }
        wake_all(wakers);
    }
}

#[derive(Debug)]
struct State<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    // Entry `p` is the shared-capacity admission limit for priority `p`.
    admission_limits: Box<[usize]>,
    available_permits: usize,
    num_waiters: usize,
    next_sequence: u64,
    groups: HashMap<Arc<K>, GroupState, S>,
    waiters: Slab<Waiter>,
}

impl<K, S> State<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(admission_limits: Box<[usize]>, hash_builder: S) -> Self {
        let total_permits = *admission_limits
            .last()
            .expect("share requires at least one admission limit");
        debug_assert!(total_permits > 0);
        Self {
            admission_limits,
            available_permits: total_permits,
            num_waiters: 0,
            next_sequence: 0,
            groups: HashMap::with_hasher(hash_builder),
            waiters: Slab::new(),
        }
    }

    fn try_admit(&mut self, key: Arc<K>, priority: usize) -> bool {
        self.validate_priority(priority);
        self.validate_owner_priority(&key, priority);
        if !self.can_admit(priority) {
            return false;
        }

        self.admit(key, priority);
        true
    }

    fn enqueue(&mut self, key: Arc<K>, priority: usize, waker: &Waker) -> usize {
        self.validate_priority(priority);
        self.validate_owner_priority(&key, priority);

        let sequence = self.next_sequence;
        self.next_sequence += 1;

        let waiter = self.waiters.insert(Waiter {
            sequence,
            waker: Some(waker.clone()),
            admitted: false,
        });
        self.groups
            .entry(key)
            .or_insert_with(|| GroupState::new(priority))
            .queue
            .push_back(waiter);
        self.num_waiters += 1;
        waiter
    }

    fn poll_waiter(&mut self, waiter: usize, waker: &Waker) -> Poll<()> {
        let state = self
            .waiters
            .get_mut(waiter)
            .expect("share waiter is missing");

        if state.admitted {
            self.waiters.remove(waiter);
            Poll::Ready(())
        } else {
            if state
                .waker
                .as_ref()
                .is_none_or(|current| !current.will_wake(waker))
            {
                state.waker = Some(waker.clone());
            }
            Poll::Pending
        }
    }

    fn cancel(&mut self, waiter_id: usize, key: &K) {
        let waiter = self.waiters.remove(waiter_id);
        if waiter.admitted {
            self.release(key);
            return;
        }

        let remove_group = {
            let group = self
                .groups
                .get_mut(key)
                .expect("share waiter group is missing");
            let position = group
                .queue
                .iter()
                .position(|candidate| *candidate == waiter_id)
                .expect("share waiter is missing from its group");
            group.queue.remove(position);
            group.held_permits == 0 && group.queue.is_empty()
        };

        self.num_waiters -= 1;
        if remove_group {
            self.groups.remove(key);
        }
    }

    fn admit_waiters(&mut self, wakers: &mut Vec<Waker>) {
        while self.available_permits > 0 && self.num_waiters > 0 {
            let Some(key) = self.next_group() else {
                return;
            };
            let waiter = self.groups[&key]
                .queue
                .front()
                .copied()
                .expect("pending share group has no waiters");
            {
                let group = self
                    .groups
                    .get_mut(&key)
                    .expect("pending share group is missing");
                let popped = group.queue.pop_front();
                debug_assert_eq!(popped, Some(waiter));
                group.held_permits += 1;
            }
            self.available_permits -= 1;
            self.num_waiters -= 1;

            let waiter = &mut self.waiters[waiter];
            waiter.admitted = true;
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
    }

    fn next_group(&self) -> Option<Arc<K>> {
        self.groups
            .iter()
            .filter_map(|(key, group)| {
                if !self.can_admit(group.priority) {
                    return None;
                }
                let waiter = *group.queue.front()?;
                let sequence = self.waiters[waiter].sequence;
                Some((Reverse(group.priority), group.held_permits, sequence, key))
            })
            .min_by_key(|(priority, held_permits, sequence, _)| {
                (*priority, *held_permits, *sequence)
            })
            .map(|(_, _, _, key)| key.clone())
    }

    fn can_admit(&self, priority: usize) -> bool {
        let total_permits = self.admission_limits[self.admission_limits.len() - 1];
        let held_permits = total_permits - self.available_permits;
        held_permits < self.admission_limits[priority]
    }

    fn admit(&mut self, key: Arc<K>, priority: usize) {
        self.groups
            .entry(key)
            .or_insert_with(|| GroupState::new(priority))
            .held_permits += 1;
        self.available_permits -= 1;
    }

    fn release(&mut self, key: &K) {
        let remove_group = {
            let group = self
                .groups
                .get_mut(key)
                .expect("share released a permit for an unknown key");
            debug_assert!(group.held_permits > 0);
            group.held_permits -= 1;
            group.held_permits == 0 && group.queue.is_empty()
        };

        if remove_group {
            self.groups.remove(key);
        }

        self.available_permits += 1;
        debug_assert!(
            self.available_permits <= self.admission_limits[self.admission_limits.len() - 1]
        );
    }

    fn validate_priority(&self, priority: usize) {
        assert!(
            priority < self.admission_limits.len(),
            "priority {priority} is outside the configured priority range 0..{}",
            self.admission_limits.len()
        );
    }

    fn validate_owner_priority(&self, key: &K, priority: usize) {
        let Some(group) = self.groups.get(key) else {
            return;
        };
        assert_eq!(
            group.priority, priority,
            "an owner cannot change priority while it has held permits or waiters"
        );
    }
}

#[derive(Debug)]
struct GroupState {
    priority: usize,
    held_permits: usize,
    queue: VecDeque<usize>,
}

impl GroupState {
    fn new(priority: usize) -> Self {
        Self {
            priority,
            held_permits: 0,
            queue: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
struct Waiter {
    sequence: u64,
    waker: Option<Waker>,
    admitted: bool,
}

#[derive(Debug)]
pub(super) struct Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    share: &'a Share<K, S>,
    key: Arc<K>,
    priority: usize,
    waiter: Option<usize>,
    completed: bool,
}

impl<'a, K, S> Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(share: &'a Share<K, S>, key: Arc<K>, priority: usize) -> Self {
        Self {
            share,
            key,
            priority,
            waiter: None,
            completed: false,
        }
    }
}

impl<K, S> Drop for Acquire<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };

        let mut wakers = Vec::new();
        {
            let mut state = self.share.state.lock();
            state.cancel(waiter, &self.key);
            state.admit_waiters(&mut wakers);
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
        if this.completed {
            return Poll::Ready(());
        }

        if let Some(waiter) = this.waiter {
            if this
                .share
                .state
                .lock()
                .poll_waiter(waiter, cx.waker())
                .is_ready()
            {
                this.waiter = None;
                this.completed = true;
                return Poll::Ready(());
            }
            return Poll::Pending;
        }

        let mut state = this.share.state.lock();
        if state.try_admit(this.key.clone(), this.priority) {
            this.completed = true;
            return Poll::Ready(());
        }

        let waiter = state.enqueue(this.key.clone(), this.priority, cx.waker());
        this.waiter = Some(waiter);
        Poll::Pending
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}
