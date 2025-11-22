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
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::condvar::Condvar;
use crate::mutex::Mutex;
use crate::test_runtime;

#[test]
fn notify_all() {
    test_runtime().block_on(async {
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let pair = Arc::new((Mutex::new(0u32), Condvar::new()));

        {
            let pair = pair.clone();
            tasks.push(tokio::spawn(async move {
                println!("I have start!");
                let (m, c) = &*pair;
                let mut count = m.lock().await;
                while *count == 0 {
                    println!("I am waiting on it");
                    count = c.wait(count).await;
                }
                *count += 1;
            }));
        }

        // Give some time for tasks to start up
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("HERE 1!");

        let (m, c) = &*pair;
        {
            let mut count = m.lock().await;
            *count += 1;
            c.notify_all();
        }

        println!("HERE 2!");

        for t in tasks {
            t.await.unwrap();
        }

        println!("HERE 3!");

        let count = m.lock().await;
        assert_eq!(2, *count);
    });
}

#[test]
fn notify_all_local_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let pair = Arc::new((Mutex::new(0u32), Condvar::new()));

        {
            let pair = pair.clone();
            tasks.push(tokio::spawn(async move {
                println!("I have start!");
                let (m, c) = &*pair;
                let mut count = m.lock().await;
                while *count == 0 {
                    println!("I am waiting on it");
                    count = c.wait(count).await;
                }
                *count += 1;
            }));
        }

        // Give some time for tasks to start up
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("HERE 1!");

        let (m, c) = &*pair;
        {
            let mut count = m.lock().await;
            *count += 1;
            c.notify_all();
        }

        println!("HERE 2!");

        for t in tasks {
            t.await.unwrap();
        }

        println!("HERE 3!");

        let count = m.lock().await;
        assert_eq!(2, *count);
    });
}
