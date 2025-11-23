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
use std::future::Future;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::internal;

/// A thread-safe cell which can nominally be written to only once.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use mea::once::OnceCell;
///
/// static CELL: OnceCell<u8> = OnceCell::new();
///
/// let handle1 = tokio::spawn(async { CELL.get_or_init(move || async { 1 }).await });
/// let handle2 = tokio::spawn(async { CELL.get_or_init(move || async { 2 }).await });
/// let result1 = handle1.await.unwrap();
/// let result2 = handle2.await.unwrap();
/// println!("Results: {}, {}", result1, result2);
/// # }
/// ```
///
/// The outputs must be either `Results: 1, 1` or `Results: 2, 2`, i.e. once the value is set via
/// an asynchronous function, the value inside the `OnceCell` will be immutable.
pub struct OnceCell<T> {
    value_set: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
    semaphore: internal::Semaphore,
}

// SAFETY: OnceCell<T> can be shared between threads as long as T is Sync + Send.
unsafe impl<T: Sync + Send> Sync for OnceCell<T> {}

// SAFETY: OnceCell<T> can be sent between threads as long as T is Send.
unsafe impl<T: Send> Send for OnceCell<T> {}


impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OnceCell<T> {
    /// Creates a new empty `OnceCell`.
    pub const fn new() -> Self {
        Self {
            value_set: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            semaphore: internal::Semaphore::new(1),
        }
    }

    /// Returns whether the internal value is set.
    fn initialized(&self) -> bool {
        self.value_set.load(Ordering::Acquire)
    }

    /// Returns the reference to the internal value or `None` if it is not set yet.
    pub fn get(&self) -> Option<&T> {
        if self.value_set.load(Ordering::Acquire) {
            Some(unsafe { self.get_unchecked() })
        } else {
            None
        }
    }

    /// Gets the reference to the internal value, initializing it with the provided asynchronous
    /// function if it is not set yet.
    ///
    /// If some other task is currently working on initializing the `OnceCell`, this call will wait
    /// for that other task to finish, then return the value that the other task produced.
    ///
    /// If the provided operation is cancelled, the initialization attempt is cancelled. If there
    /// are other tasks waiting for the value to be initialized, one of them will start another
    /// attempt at initializing the value.
    ///
    /// This will deadlock if `init` tries to initialize the cell recursively.
    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if self.initialized() {
            // SAFETY: We just checked that the value is initialized.
            return unsafe { self.get_unchecked() };
        }
        self.semaphore.acquire(1).await;
        let _guard = Guard {
            semaphore: &self.semaphore,
        };
        if self.initialized() {
            // Another task initialized the value while we were waiting for the semaphore.
            // SAFETY: We just checked that the value is initialized.
            return unsafe { self.get_unchecked() };
        }

        let value = init().await;
        let value_ptr = self.value.get();
        unsafe { value_ptr.write(MaybeUninit::new(value)) };
        // Use `store` with `Release` ordering to ensure that when loading it with `Acquire`
        // ordering, the initialized value is visible.
        self.value_set.store(true, Ordering::Release);
        // SAFETY: value initialized one line above
        unsafe { self.get_unchecked() }
    }

    // SAFETY: Caller must ensure that `initialized()` returns true before calling this method.
    unsafe fn get_unchecked(&self) -> &T {
        unsafe { &*(*self.value.get()).as_ptr() }
    }
}

impl<T> Drop for OnceCell<T> {
    fn drop(&mut self) {
        if self.initialized() {
            let value = self.value.get_mut().as_mut_ptr();
            // SAFETY: the value is initialized
            unsafe { std::ptr::drop_in_place(value) };
        }
    }
}

struct Guard<'a> {
    semaphore: &'a internal::Semaphore,
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        self.semaphore.release(1);
    }
}
