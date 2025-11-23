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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::Mutex;

use super::once_cell::OnceCell;
use crate::latch::Latch;

struct Foo {
    value: Arc<AtomicU32>,
}

async fn init_foo(value: Arc<AtomicU32>) -> Foo {
    // Simulate latency in initialization
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    Foo { value }
}

impl Drop for Foo {
    fn drop(&mut self) {
        self.value.fetch_add(1, Ordering::Release);
    }
}

impl From<Arc<AtomicU32>> for Foo {
    fn from(value: Arc<AtomicU32>) -> Self {
        Self { value }
    }
}

#[tokio::test]
async fn drop_cell() {
    let num_drops = Arc::new(AtomicU32::new(0));
    {
        let once_cell = OnceCell::new();
        assert!(once_cell.get().is_none());
        let num_drops_clone = num_drops.clone();
        once_cell
            .get_or_init(move || async move { init_foo(num_drops_clone).await })
            .await;
        assert!(once_cell.get().unwrap().value.load(Ordering::Acquire) == 0);
        assert!(num_drops.load(Ordering::Acquire) == 0);
    }
    assert!(num_drops.load(Ordering::Acquire) == 1);
}

#[test]
fn multi_init() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let cell = Arc::new(OnceCell::new());
        const N: usize = 100;
        let latch = Arc::new(Latch::new(N as u32));
        let values = Arc::new(Mutex::new(vec![0; N]));

        for i in 0..N {
            let cell_clone = cell.clone();
            let latch = latch.clone();
            let values = values.clone();
            rt.spawn(async move {
                let result = cell_clone
                    .get_or_init(move || async move { i + 1000 })
                    .await;
                let mut values = values.lock().await;
                if let Some(item) = values.get_mut(i) {
                    *item = *result;
                }
                latch.count_down();
            });
        }
        latch.wait().await;
        let cell_value = cell.get().unwrap();
        println!("Cell value: {}", *cell_value);
        for (index, value) in values.lock().await.iter().enumerate() {
            assert_eq!(
                *value, *cell_value,
                "mismatch at index {}, expected: {}, actual: {}",
                index, *cell_value, *value
            );
        }
    });
}
