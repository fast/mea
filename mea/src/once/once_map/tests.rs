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

use crate::once::OnceMap;

#[tokio::test]
async fn test_compute() {
    let map = OnceMap::new();
    let v = map.compute("key", async || 1).await;
    assert_eq!(v, 1);
    let v = map.compute("key", async || 2).await;
    assert_eq!(v, 1);
}

#[tokio::test]
async fn test_compute_concurrent() {
    let map = Arc::new(OnceMap::new());
    let cnt = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let map = map.clone();
        let cnt = cnt.clone();
        handles.push(tokio::spawn(async move {
            map.compute("key", async move || {
                cnt.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                42
            })
            .await
        }));
    }

    for h in handles {
        assert_eq!(h.await.unwrap(), 42);
    }
    assert_eq!(cnt.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_try_compute() {
    let map = OnceMap::new();

    // Fail first
    let res: Result<i32, &str> = map.try_compute("key", async || Err("fail")).await;
    assert_eq!(res, Err("fail"));

    // Success then
    let res: Result<i32, &str> = map.try_compute("key", async || Ok::<i32, &str>(1)).await;
    assert_eq!(res, Ok(1));

    // Cached
    let res: Result<i32, &str> = map.try_compute("key", async || Ok::<i32, &str>(2)).await;
    assert_eq!(res, Ok(1));
}

#[tokio::test]
async fn test_get_remove() {
    let map = OnceMap::new();
    assert_eq!(map.get("key"), None);
    assert_eq!(map.remove("key"), None);

    map.compute("key", async || 1).await;
    assert_eq!(map.get("key"), Some(1));

    let v = map.remove("key");
    assert_eq!(v, Some(1));
    assert_eq!(map.get("key"), None);

    map.compute("key", async || 2).await;
    map.discard("key");
    assert_eq!(map.get("key"), None);
}

#[tokio::test]
async fn test_from_iter() {
    let map: OnceMap<_, _> = vec![("a", 1), ("b", 2)].into_iter().collect();
    assert_eq!(map.get("a"), Some(1));
    assert_eq!(map.get("b"), Some(2));
    assert_eq!(map.get("c"), None);
}
