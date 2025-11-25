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
use std::fmt;
use std::fmt::Display;
use std::future::Future;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::semaphore::Semaphore;
use crate::semaphore::SemaphorePermit;

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
    semaphore: Semaphore,
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
            semaphore: Semaphore::new(1),
        }
    }

    /// Creates a new `OnceCell` initialized with the provided value.
    pub const fn from_value(value: T) -> Self {
        Self {
            value_set: AtomicBool::new(true),
            value: UnsafeCell::new(MaybeUninit::new(value)),
            semaphore: Semaphore::new(1),
        }
    }

    /// Returns whether the internal value is set.
    fn is_initialized(&self) -> bool {
        self.value_set.load(Ordering::Acquire)
    }

    /// Gets the reference to the underlying value.
    ///
    /// Returns `None` if the cell is uninitialized, or being initialized.
    ///
    /// This method never blocks.
    pub fn get(&self) -> Option<&T> {
        if self.is_initialized() {
            // SAFETY: checked is_initialized
            Some(unsafe { self.get_unchecked() })
        } else {
            None
        }
    }

    /// Gets the mutable reference to the underlying value.
    ///
    /// Returns `None` if the cell is uninitialized.
    ///
    /// This method never blocks. Since it borrows the `OnceCell` mutably, it is statically
    /// guaranteed that no active borrows to the `OnceCell` exist, including from other threads.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        if self.is_initialized() {
            // SAFETY: checked is_initialized and we have a unique access
            Some(unsafe { self.get_unchecked_mut() })
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
        if let Some(v) = self.get() {
            return v;
        }

        let permit = self.semaphore.acquire(1).await;

        if let Some(v) = self.get() {
            // double-checked: another task initialized the value
            // while we were waiting for the permit
            return v;
        }

        let value = init().await;
        self.set_value(value, permit)
    }

    /// Gets the reference to the internal value, initializing it with the provided asynchronous
    /// function if it is not set yet.
    ///
    /// If some other task is currently working on initializing the `OnceCell`, this call will wait
    /// for that other task to finish, then return the value that the other task produced.
    ///
    /// If the provided operation returns an error, is cancelled or panics, the initialization
    /// attempt is cancelled. If there are other tasks waiting for the value to be initialized
    /// one of them will start another attempt at initializing the value.
    ///
    /// This will deadlock if `init` tries to initialize the cell recursively.
    pub async fn get_or_try_init<E, F, Fut>(&self, init: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(v) = self.get() {
            return Ok(v);
        }

        let permit = self.semaphore.acquire(1).await;

        if let Some(v) = self.get() {
            // double-checked: another task initialized the value
            // while we were waiting for the permit
            return Ok(v);
        }

        match init().await {
            Ok(v) => Ok(self.set_value(v, permit)),
            Err(err) => Err(err),
        }
    }

    /// Sets the value of the cell to the given value if `OnceCell` is empty.
    ///
    /// If the cell is already initialized, returns a [`SetError::AlreadyInitializedError`].
    ///
    /// If the cell is currently being initialized by another task, returns a
    /// [`SetError::InitializingError`].
    ///
    /// [`SetError::AlreadyInitializedError`]: crate::once::SetError::AlreadyInitializedError
    /// [`SetError::InitializingError`]: crate::once::SetError::InitializingError
    pub fn set(&self, value: T) -> Result<(), SetError<T>> {
        if self.is_initialized() {
            return Err(SetError::AlreadyInitializedError(value));
        }

        match self.semaphore.try_acquire(1) {
            Some(permit) => {
                if self.is_initialized() {
                    // this case should hardly happen, but we check again to be safe
                    Err(SetError::InitializingError(value))
                } else {
                    self.set_value(value, permit);
                    Ok(())
                }
            }
            None => Err(SetError::InitializingError(value)),
        }
    }

    /// # Safety
    ///
    /// The cell must be initialized
    #[inline]
    unsafe fn get_unchecked(&self) -> &T {
        debug_assert!(self.is_initialized());
        unsafe { (&*self.value.get()).assume_init_ref() }
    }

    /// # Safety
    ///
    /// The cell must be initialized
    #[inline]
    unsafe fn get_unchecked_mut(&mut self) -> &mut T {
        debug_assert!(self.is_initialized());
        unsafe { (&mut *self.value.get()).assume_init_mut() }
    }

    fn set_value(&self, value: T, permit: SemaphorePermit<'_>) -> &T {
        // Hold the permit to ensure exclusive access.
        let _permit = permit;

        let value_ptr = self.value.get();
        unsafe { value_ptr.write(MaybeUninit::new(value)) };

        // Use `store` with `Release` ordering to ensure that when loading it with `Acquire`
        // ordering, the initialized value is visible.
        self.value_set.store(true, Ordering::Release);

        // SAFETY: value initialized above
        unsafe { self.get_unchecked() }
    }
}

impl<T> Drop for OnceCell<T> {
    fn drop(&mut self) {
        if *self.value_set.get_mut() {
            // SAFETY: The cell is initialized and being dropped, so it can't be accessed again.
            unsafe { (&mut *self.value.get()).assume_init_drop() };
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for OnceCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_tuple("OnceCell");
        match self.get() {
            Some(v) => d.field(v),
            None => d.field(&format_args!("<uninit>")),
        };
        d.finish()
    }
}

impl<T: Clone> Clone for OnceCell<T> {
    fn clone(&self) -> OnceCell<T> {
        match self.get() {
            Some(v) => OnceCell::from_value(v.clone()),
            None => OnceCell::new(),
        }
    }
}

impl<T> From<T> for OnceCell<T> {
    fn from(value: T) -> Self {
        OnceCell::from_value(value)
    }
}

impl<T: PartialEq> PartialEq for OnceCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq> Eq for OnceCell<T> {}

/// Errors that can be returned from [`OnceCell::set`].
///
/// [`OnceCell::set`]: crate::once::OnceCell::set
#[derive(Debug, PartialEq, Eq)]
pub enum SetError<T> {
    /// The cell was already initialized when [`OnceCell::set`] was called.
    ///
    /// [`OnceCell::set`]: crate::once::OnceCell::set
    AlreadyInitializedError(T),

    /// The cell is currently being initialized.
    InitializingError(T),
}

impl<T> Display for SetError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetError::AlreadyInitializedError(_) => {
                write!(f, "AlreadyInitializedError")
            }
            SetError::InitializingError(_) => {
                write!(f, "InitializingError")
            }
        }
    }
}
