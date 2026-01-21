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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::singleflight::Group;

#[tokio::test]
async fn test_simple() {
    let group = Group::new();
    let res = group.work("key", || async { "val" }).await;
    assert_eq!(res, "val");
}

#[tokio::test]
async fn test_coalescing() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let group = group.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            group
                .work("key", || async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    "val"
                })
                .await
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.unwrap(), "val");
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multiple_keys() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let g1 = group.clone();
    let c1 = counter.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key1", || async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c1.fetch_add(1, Ordering::SeqCst);
            "val1"
        })
        .await
    });

    let g2 = group.clone();
    let c2 = counter.clone();
    let h2 = tokio::spawn(async move {
        g2.work("key2", || async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c2.fetch_add(1, Ordering::SeqCst);
            "val2"
        })
        .await
    });

    assert_eq!(h1.await.unwrap(), "val1");
    assert_eq!(h2.await.unwrap(), "val2");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_forget() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let g1 = group.clone();
    let c1 = counter.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key", || async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c1.fetch_add(1, Ordering::SeqCst);
            "val1"
        })
        .await
    });

    // Wait a bit to ensure the first call is established
    tokio::time::sleep(Duration::from_millis(10)).await;
    group.forget(&"key");

    let g2 = group.clone();
    let c2 = counter.clone();
    let h2 = tokio::spawn(async move {
        g2.work("key", || async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c2.fetch_add(1, Ordering::SeqCst);
            "val2"
        })
        .await
    });

    assert_eq!(h1.await.unwrap(), "val1");
    assert_eq!(h2.await.unwrap(), "val2");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_panic_safe() {
    let group = Arc::new(Group::<&str, String>::new());

    // Task that panics
    let g1 = group.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key", || async {
            panic!("oops");
        })
        .await
    });

    // Wait for h1 to panic and exit
    let err = h1.await.unwrap_err();
    assert!(err.is_panic());

    // Next task should succeed (new attempt)
    let res = group.work("key", || async { "success".to_string() }).await;
    assert_eq!(res, "success");
}

#[tokio::test]
async fn test_try_work_simple() {
    let group = Group::new();
    let res = group
        .try_work("key", || async { Ok::<&str, ()>("val") })
        .await;
    assert_eq!(res, Ok("val"));

    // Should be removed from map, so next call executes again
    let res2 = group
        .try_work("key", || async { Ok::<&str, ()>("val2") })
        .await;
    assert_eq!(res2, Ok("val2"));
}

#[tokio::test]
async fn test_try_work_coalescing() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let group = group.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            group
                .try_work("key", || async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok::<&str, ()>("val")
                })
                .await
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.unwrap(), Ok("val"));
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_try_work_failure() {
    let group = Group::new();
    let res = group
        .try_work("key", || async { Err::<&str, &str>("error") })
        .await;
    assert_eq!(res, Err("error"));

    // Retry should work
    let res2 = group
        .try_work("key", || async { Ok::<&str, ()>("success") })
        .await;
    assert_eq!(res2, Ok("success"));
}

#[tokio::test]
async fn test_try_work_wait_and_retry() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let g1 = group.clone();
    let c1 = counter.clone();
    let h1 = tokio::spawn(async move {
        g1.try_work("key", || async move {
            c1.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Err::<&str, &str>("fail")
        })
        .await
    });

    let g2 = group.clone();
    let c2 = counter.clone();
    let h2 = tokio::spawn(async move {
        // Ensure h1 starts first
        tokio::time::sleep(Duration::from_millis(10)).await;
        g2.try_work("key", || async move {
            c2.fetch_add(1, Ordering::SeqCst);
            Ok::<&str, ()>("success")
        })
        .await
    });

    assert_eq!(h1.await.unwrap(), Err("fail"));
    assert_eq!(h2.await.unwrap(), Ok("success"));
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}
