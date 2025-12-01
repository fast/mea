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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Mutex;

use super::Once;
use super::once_cell::OnceCell;
use crate::latch::Latch;
use crate::test_runtime;

struct Foo {
    value: Arc<AtomicBool>,
}

impl Foo {
    async fn new(value: Arc<AtomicBool>) -> Self {
        // simulate some async initialization work
        tokio::time::sleep(Duration::from_millis(100)).await;
        Foo { value }
    }

    fn value(&self) -> bool {
        self.value.load(Ordering::Acquire)
    }
}

impl Drop for Foo {
    fn drop(&mut self) {
        self.value.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn drop_cell() {
    let dropped = Arc::new(AtomicBool::new(false));

    {
        let cell = OnceCell::new();
        assert!(cell.get().is_none());

        let state = dropped.clone();
        cell.get_or_init(|| async { Foo::new(state).await }).await;

        let foo = cell.get().unwrap();
        assert!(!foo.value());
        assert!(!dropped.load(Ordering::Acquire));
    }

    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn multi_init() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();

    static CELL: OnceCell<usize> = OnceCell::new();

    rt.block_on(async {
        const N: usize = 100;

        let latch = Arc::new(Latch::new(N as u32));
        let values = Arc::new(Mutex::new(vec![0; N]));

        for i in 0..N {
            let latch = latch.clone();
            let values = values.clone();
            rt.spawn(async move {
                let result = CELL.get_or_init(move || async move { i + 1000 }).await;
                let mut values = values.lock().await;
                values[i] = *result;
                latch.count_down();
            });
        }

        latch.wait().await;
        let cell_value = CELL.get().unwrap();
        for (index, value) in values.lock().await.iter().enumerate() {
            assert_eq!(*value, *cell_value, "mismatch at index {index}");
        }
    });
}

#[tokio::test]
async fn init_cancelled() {
    static CELL: OnceCell<u8> = OnceCell::new();

    let handle1 = tokio::spawn(async {
        let fut = CELL.get_or_init(|| async {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            1
        });
        let timeout = tokio::time::timeout(Duration::from_millis(1), fut).await;
        assert!(timeout.is_err());
    });

    let handle2 = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let value = CELL.get_or_init(|| async { 2 }).await;
        assert_eq!(*value, 2);
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
}

#[tokio::test]
async fn init_error() {
    {
        static CELL: OnceCell<u8> = OnceCell::new();

        let handle1 = tokio::spawn(async {
            let result = CELL.get_or_try_init(|| async { Err(()) }).await;
            assert!(result.is_err());
        });

        let handle2 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let value = CELL.get_or_try_init(|| async { Ok::<_, ()>(2) }).await;
            assert_eq!(*value.unwrap(), 2);
        });

        handle1.await.unwrap();
        handle2.await.unwrap();
    }

    {
        static CELL: OnceCell<u8> = OnceCell::new();

        let handle1 = tokio::spawn(async {
            let value = CELL.get_or_try_init(|| async { Ok::<_, ()>(2) }).await;
            assert_eq!(*value.unwrap(), 2);
        });

        let handle2 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let value = CELL.get_or_try_init(|| async { Err(()) }).await;
            assert_eq!(*value.unwrap(), 2);
        });

        handle1.await.unwrap();
        handle2.await.unwrap();
    }
}

#[tokio::test]
async fn get_mut_or_init() {
    let mut cell: OnceCell<u32> = OnceCell::new();
    let v = cell
        .get_mut_or_init(async || {
            tokio::time::sleep(Duration::from_millis(1)).await;
            41
        })
        .await;
    *v += 1;

    let v = tokio::spawn(async move { *cell.get_or_init(async || 0).await })
        .await
        .unwrap();
    assert_eq!(v, 42);
}

#[tokio::test]
async fn get_mut_or_try_init() {
    let mut cell: OnceCell<u32> = OnceCell::new();
    let r = cell
        .get_mut_or_try_init(async || {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Err(())
        })
        .await;
    assert!(r.is_err());
    assert_eq!(cell.get_mut(), None);

    let v = tokio::spawn(async move {
        let v = cell
            .get_mut_or_try_init(async || Ok::<_, ()>(10))
            .await
            .unwrap();
        *v += 5;
        *v
    })
    .await
    .unwrap();
    assert_eq!(v, 15);
}

#[tokio::test]
async fn test_call_once_runs_only_once() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    assert!(!ONCE.is_completed());

    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert!(ONCE.is_completed());
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

    // Second call should not run the closure
    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
}

#[test]
fn test_once_multi_task() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    test_runtime().block_on(async {
        const N: usize = 100;

        let latch = Arc::new(Latch::new(N as u32));
        let mut handles = Vec::with_capacity(N);

        for _ in 0..N {
            let latch = latch.clone();
            handles.push(tokio::spawn(async move {
                ONCE.call_once(async || {
                    COUNTER.fetch_add(1, Ordering::SeqCst);
                })
                .await;
                latch.count_down();
            }));
        }

        latch.wait().await;

        for handle in handles {
            handle.await.unwrap();
        }

        // Only one task should have incremented the counter
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
        assert!(ONCE.is_completed());
    });
}

#[tokio::test]
async fn test_once_cancelled() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let handle1 = tokio::spawn(async {
        let fut = ONCE.call_once(async || {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            COUNTER.fetch_add(1, Ordering::SeqCst);
        });
        let timeout = tokio::time::timeout(Duration::from_millis(1), fut).await;
        assert!(timeout.is_err());
    });

    let handle2 = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ONCE.call_once(async || {
            COUNTER.fetch_add(10, Ordering::SeqCst);
        })
        .await;
    });

    handle1.await.unwrap();
    handle2.await.unwrap();

    // The second task should have run since the first was cancelled
    assert_eq!(COUNTER.load(Ordering::SeqCst), 10);
    assert!(ONCE.is_completed());
}

#[tokio::test]
async fn test_once_debug() {
    let once = Once::new();
    let debug_str = format!("{:?}", once);
    assert!(debug_str.contains("Once"));
    assert!(debug_str.contains("done"));
    assert!(debug_str.contains("false"));

    once.call_once(async || {}).await;

    let debug_str = format!("{:?}", once);
    assert!(debug_str.contains("true"));
}

#[tokio::test]
async fn test_once_default() {
    let once = Once::default();
    assert!(!once.is_completed());
}

#[tokio::test]
async fn test_once_retry_after_panic() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let handle = tokio::spawn(async {
        ONCE.call_once(async || {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            panic!("boom");
        })
        .await;
    });

    let err = handle.await.expect_err("once init should panic");
    assert!(err.is_panic());

    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
    assert!(ONCE.is_completed());
}
