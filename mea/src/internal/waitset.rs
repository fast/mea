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

use std::task::Context;
use std::task::Waker;

use slab::Slab;

/// Identifies a registered waker in a [`WaitSet`].
///
/// The generation distinguishes reused slab slots so stale waiter IDs cannot remove or update a
/// newer waiter that happens to occupy the same index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WaiterId {
    index: usize,
    generation: u64,
}

#[derive(Debug)]
struct Waiter {
    generation: u64,
    waker: Waker,
}

#[derive(Debug)]
pub(crate) struct WaitSet {
    waiters: Slab<Waiter>,
    next_generation: u64,
}

impl WaitSet {
    /// Construct a new, empty wait set.
    pub const fn new() -> Self {
        Self {
            waiters: Slab::new(),
            next_generation: 0,
        }
    }

    /// Construct a new, empty wait set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            waiters: Slab::with_capacity(capacity),
            next_generation: 0,
        }
    }

    /// Drains all wakers from the wait set.
    ///
    /// Callers should release any lock that protects the wait set before calling [`Waker::wake`] on
    /// the returned wakers, to avoid running user-provided waker code with that lock held.
    pub(crate) fn take_wakers(&mut self) -> Vec<Waker> {
        self.waiters.drain().map(|waiter| waiter.waker).collect()
    }

    /// Removes a previously registered waker from the wait set, returning it if it was still
    /// registered.
    ///
    /// The returned waker should be dropped after releasing any lock that protects the wait set,
    /// so that user-provided `Waker::Drop` does not run with that lock held.
    pub(crate) fn remove_waker(&mut self, id: &mut Option<WaiterId>) -> Option<Waker> {
        let key = id.take()?;
        let waiter = self.waiters.get(key.index)?;

        if waiter.generation == key.generation {
            Some(self.waiters.remove(key.index).waker)
        } else {
            None
        }
    }

    /// Registers a waker to the wait set.
    ///
    /// `id` must be `None` when the waker is not registered, or `Some` with a waiter ID previously
    /// stored by this method.
    ///
    /// If a stored waker is replaced by a different one, the old waker is returned. The caller
    /// should drop the returned waker after releasing any lock that protects the wait set, so
    /// that user-provided `Waker::Drop` does not run with that lock held.
    pub(crate) fn register_waker(
        &mut self,
        id: &mut Option<WaiterId>,
        cx: &mut Context<'_>,
    ) -> Option<Waker> {
        if let Some(key) = *id {
            if let Some(waiter) = self.waiters.get_mut(key.index) {
                if waiter.generation == key.generation {
                    if !waiter.waker.will_wake(cx.waker()) {
                        return Some(std::mem::replace(&mut waiter.waker, cx.waker().clone()));
                    }
                    return None;
                }
            }
        }

        // The stored WaiterId may be stale if the waiter was removed, drained by `take_wakers`,
        // or if the slab slot has since been reused by another waiter. Register the current waker
        // again and replace the stale ID.
        *id = Some(self.insert_waker(cx.waker()));
        None
    }

    /// Allocates a fresh waiter ID and stores the waker in the wait set.
    fn insert_waker(&mut self, waker: &Waker) -> WaiterId {
        let generation = self.next_generation;

        // Do not wrap the generation counter: wrapping could make a stale WaiterId valid again
        // after enough insertions.
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("wait set generation counter overflowed");

        let index = self.waiters.insert(Waiter {
            generation,
            waker: waker.clone(),
        });
        WaiterId { index, generation }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    use super::WaitSet;

    struct DropWake {
        dropped: Arc<AtomicBool>,
        wake_count: AtomicUsize,
    }

    impl Wake for DropWake {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Drop for DropWake {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    fn drop_waker() -> (Waker, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(DropWake {
            dropped: dropped.clone(),
            wake_count: AtomicUsize::new(0),
        }));
        (waker, dropped)
    }

    #[test]
    fn test_remove_waker_delays_drop() {
        let mut waiters = WaitSet::new();
        let mut id = None;
        let (waker, dropped) = drop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(waiters.register_waker(&mut id, &mut cx).is_none());
        drop(waker);

        let removed = waiters.remove_waker(&mut id).unwrap();
        assert!(!dropped.load(Ordering::Relaxed));

        drop(removed);
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[test]
    fn test_replace_waker_delays_drop() {
        let mut waiters = WaitSet::new();
        let mut id = None;
        let (waker, dropped) = drop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(waiters.register_waker(&mut id, &mut cx).is_none());
        drop(waker);

        let (replacement, _) = drop_waker();
        let mut cx = Context::from_waker(&replacement);
        let replaced = waiters.register_waker(&mut id, &mut cx).unwrap();
        assert!(!dropped.load(Ordering::Relaxed));

        drop(replaced);
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[test]
    fn test_stale_waiter_id_does_not_remove_reused_slot() {
        let mut waiters = WaitSet::new();
        let mut stale_id = None;
        let (waker, _) = drop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(waiters.register_waker(&mut stale_id, &mut cx).is_none());
        let stale_id_value = stale_id.unwrap();
        drop(waiters.take_wakers());

        let mut current_id = None;
        assert!(waiters.register_waker(&mut current_id, &mut cx).is_none());
        let current_id_value = current_id.unwrap();
        assert_eq!(stale_id_value.index, current_id_value.index);
        assert_ne!(stale_id_value.generation, current_id_value.generation);

        assert!(waiters.remove_waker(&mut stale_id).is_none());
        assert!(waiters.remove_waker(&mut current_id).is_some());
    }
}
