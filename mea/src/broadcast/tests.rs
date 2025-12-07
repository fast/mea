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

use super::*;

#[tokio::test]
async fn test_broadcast_basic() {
    let (tx, mut rx1) = channel(10);
    let mut rx2 = rx1.clone();

    tx.send(10);
    tx.send(20);

    assert_eq!(rx1.recv().await, Ok(10));
    assert_eq!(rx1.recv().await, Ok(20));
    assert_eq!(rx2.recv().await, Ok(10));
    assert_eq!(rx2.recv().await, Ok(20));
}

#[tokio::test]
async fn test_broadcast_lagged() {
    let (tx, mut rx) = channel(2); // Capacity 2

    tx.send(1);
    tx.send(2);
    tx.send(3); // Overwrites 1. Rx lagged by 1 (missed msg '1').

    // Rx should return Lagged(1) and catch up to 2 (oldest valid).
    match rx.recv().await {
        Err(RecvError::Lagged(n)) => assert_eq!(n, 1),
        _ => panic!("Expected Lagged error"),
    }

    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
}

#[tokio::test]
async fn test_broadcast_lagged_multi() {
    let (tx, mut rx) = channel(2);

    tx.send(1);
    tx.send(2);
    tx.send(3);
    tx.send(4);
    // Overwrites 1 and 2.
    // Buffer has [3, 4]. Tail is 4. Head is 0.
    // Diff is 4. Cap is 2.
    // Missed = (Tail - Cap) - Head = (4 - 2) - 0 = 2.
    // Oldest valid is 2 (msg '3'). Wait...
    // Tail=4.
    // Slot indices: 0->1, 1->2, 0->3, 1->4.
    // Buffer at 0 is 3 (ver 2). Buffer at 1 is 4 (ver 3).
    // Spec says: "global_tail: Tracks the next write position".
    // Send 1: buffer[0]=1, tail=1.
    // Send 2: buffer[1]=2, tail=2.
    // Send 3: buffer[0]=3, tail=3.
    // Send 4: buffer[1]=4, tail=4.
    // Oldest valid is at tail-cap = 4-2 = 2.
    // Message at version 2 is '3'.
    // So we missed version 0 ('1') and version 1 ('2'). Missed count = 2.

    match rx.recv().await {
        Err(RecvError::Lagged(n)) => assert_eq!(n, 2),
        _ => panic!("Expected Lagged error"),
    }

    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(rx.recv().await, Ok(4));
}

#[tokio::test]
async fn test_broadcast_closed() {
    let (tx, mut rx) = channel::<i32>(10);
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Closed));
}

#[tokio::test]
async fn test_wrap_around() {
    let (tx, mut rx) = channel(2);
    // Send enough to wrap u64? No, just buffer wrap.
    for i in 0..10 {
        tx.send(i);
        if i >= 2 {
            // Consume to prevent lag, or just check lag handling repeatedly
            if let Ok(v) = rx.recv().await {
                assert_eq!(v, i);
            }
        }
    }
}

#[tokio::test]
async fn test_wait_mechanism() {
    let (tx, mut rx) = channel(10);

    let handle = tokio::spawn(async move { rx.recv().await });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    tx.send(42);

    assert_eq!(handle.await.unwrap(), Ok(42));
}
