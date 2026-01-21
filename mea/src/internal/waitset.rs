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

#[derive(Debug)]
pub(crate) struct WaitSet {
    waiters: Vec<Waker>,
}

impl WaitSet {
    /// Construct a new, empty wait set.
    pub const fn new() -> Self {
        Self {
            waiters: Vec::new(),
        }
    }

    /// Construct a new, empty wait set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            waiters: Vec::with_capacity(capacity),
        }
    }

    /// Drain and wake up all waiters.
    pub(crate) fn wake_all(&mut self) {
        for w in self.waiters.drain(..) {
            w.wake();
        }
    }

    /// Registers a waker to the wait set.
    ///
    /// `idx` must be `None` when the waker is not registered, or `Some(key)` where `key` is
    /// a value previously returned by this method.
    pub(crate) fn register_waker(&mut self, idx: &mut Option<usize>, cx: &mut Context<'_>) {
        match *idx {
            None => {
                self.waiters.push(cx.waker().clone());
                *idx = Some(self.waiters.len() - 1);
            }
            Some(key) => {
                if key < self.waiters.len() {
                    if !self.waiters[key].will_wake(cx.waker()) {
                        self.waiters[key] = cx.waker().clone();
                    }
                } else {
                    // DEFENSIVE NOTE:
                    //
                    // This is possible if latch/waitgroup is fired between the first and second
                    // state check.
                    //
                    // In this case, it does not harm to re-register the waker. Because
                    // the second state check will finish the future and the WaitSet gets
                    // dropped.
                    //
                    // Barrier holds the lock during check and register, so the race condition
                    // above won't happen.
                    self.waiters.push(cx.waker().clone());
                    *idx = Some(self.waiters.len() - 1);
                }
            }
        }
    }
}
