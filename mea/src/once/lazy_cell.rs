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

use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::semaphore::Semaphore;

union Data<T, F> {
    value: ManuallyDrop<T>,
    f: ManuallyDrop<F>,
}

///
pub struct LazyCell<T, F> {
    value_set: AtomicBool,
    data: UnsafeCell<Data<T, F>>,
    semaphore: Semaphore,
}

impl<T, F: AsyncFnOnce() -> T> LazyCell<T, F> {
    ///
    pub const fn new(f: F) -> LazyCell<T, F> {
        LazyCell {
            value_set: AtomicBool::new(false),
            data: UnsafeCell::new(Data {
                f: ManuallyDrop::new(f),
            }),
            semaphore: Semaphore::new(1),
        }
    }

    /// Returns whether the internal value is set.
    fn initialized(&self) -> bool {
        self.value_set.load(Ordering::Acquire)
    }

    ///
    pub fn get(&self) -> Option<&T> {
        if self.initialized() {
            Some(unsafe { &(*self.data.get()).value })
        } else {
            None
        }
    }

    ///
    pub async fn get_or_init(&self) -> &T {
        if let Some(v) = self.get() {
            return v;
        }

        let _permit = self.semaphore.acquire(1).await;

        if let Some(v) = self.get() {
            // double-checked: another task initialized the value
            // while we were waiting for the permit
            return v;
        }

        let data = unsafe { &mut *self.data.get() };
        let f = unsafe { ManuallyDrop::take(&mut data.f) };
        let value = f().await;
        data.value = ManuallyDrop::new(value);
        self.value_set.store(true, Ordering::Release);
        unsafe { &*(*self.data.get()).value }
    }
}

#[cfg(test)]
#[tokio::test]
async fn test_lazy_cell() {
    let cell = LazyCell::new(|| async {
        println!("initializing...");
        42
    });

    let v1 = cell.get_or_init().await;
    assert_eq!(*v1, 42);
}
