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
    // Overwrites 1 and 2. Missed 2 messages.

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
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn test_wrap_around() {
    let (tx, mut rx) = channel(2);
    for i in 0..10 {
        tx.send(i);
        if i >= 2 {
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

#[tokio::test]
async fn test_subscribe() {
    let (tx, _rx) = channel::<i32>(10);
    let mut rx = tx.subscribe();

    tx.send(100);
    assert_eq!(rx.recv().await, Ok(100));
}

#[tokio::test]
async fn test_resubscribe() {
    let (tx, mut rx) = channel(2);

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();

    // rx sees 1, 2
    // rx2 sees nothing yet (starts at tail=2)

    tx.send(3);

    match rx.recv().await {
        Err(RecvError::Lagged(n)) => assert_eq!(n, 1),
        _ => panic!("Expected Lagged error"),
    }
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx2.recv().await, Ok(3));
}

#[tokio::test]
async fn test_overflow() {
    let (tx, mut rx) = channel(4);
    let mut rx2 = rx.clone();

    let boundary = u64::MAX - 2;
    tx.shared.tail_cnt.store(boundary, Ordering::SeqCst);
    rx.head = boundary;

    tx.send(1);
    assert_eq!(rx.recv().await, Ok(1));

    tx.send(2);
    tx.send(3);
    tx.send(4);
    tx.send(5);
    tx.send(6);
    tx.send(7);
    tx.send(8);

    assert_eq!(rx.recv().await, Err(RecvError::Lagged(3)));
    assert_eq!(rx.recv().await, Ok(5));
    assert_eq!(rx.recv().await, Ok(6));
    assert_eq!(rx.recv().await, Ok(7));
    assert_eq!(rx.recv().await, Ok(8));

    assert_eq!(rx2.recv().await, Err(RecvError::Lagged(1)));
    assert_eq!(rx2.recv().await, Ok(5));
    assert_eq!(rx2.recv().await, Ok(6));
    assert_eq!(rx2.recv().await, Ok(7));
    assert_eq!(rx2.recv().await, Ok(8));
}

#[tokio::test]
async fn test_overflow_wrapping() {
    let (tx, mut rx) = channel(4);
    let mut rx2 = rx.clone();

    let boundary = u64::MAX - 2;
    tx.shared.tail_cnt.store(boundary, Ordering::SeqCst);
    rx.head = boundary;

    tx.send(1);
    assert_eq!(rx.recv().await, Ok(1));

    tx.send(2);
    tx.send(3);
    tx.send(4);
    tx.send(5);

    assert_eq!(rx.recv().await, Ok(2));
    // Note: wrapping just hit the head.
    // This requires the tail to wrap around the entire u64 space (approx 584 years at 10^9 msg/s),
    // which effectively creates an ABA problem where version 0 (wrapped) looks like version 0
    // (start). This is a known limitation of the wrapping arithmetic logic, accepted for
    // performance reasons as it is practically impossible to trigger without manually setting
    // the tail.
    assert_eq!(rx2.recv().await, Ok(4));
}

#[tokio::test]
async fn test_capacity_rounding() {
    let (tx, _) = channel::<()>(3);
    assert_eq!(tx.shared.capacity, 4);
    assert_eq!(tx.shared.mask, 3);

    let (tx, _) = channel::<()>(4);
    assert_eq!(tx.shared.capacity, 4);
    assert_eq!(tx.shared.mask, 3);

    let (tx, _) = channel::<()>(5);
    assert_eq!(tx.shared.capacity, 8);
    assert_eq!(tx.shared.mask, 7);
}

#[tokio::test]
async fn test_try_recv() {
    let (tx, mut rx) = channel(16);

    // Empty
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Success
    tx.send(10);
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Closed
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn test_try_recv_lagged() {
    let (tx, mut rx) = channel(2);
    tx.send(1);
    tx.send(2);
    tx.send(3);

    assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(1)));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn test_broadcast_overflow_and_lagged() {
    let cap = 4;
    let (tx, mut rx) = channel(cap);
    let mut rx2 = rx.clone();

    // Sender task: rapidly send many messages, exceeding capacity
    let sender_tx = tx.clone();
    let sender_handle = tokio::spawn(async move {
        for i in 0..100 {
            sender_tx.send(i);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    });

    sender_handle.await.unwrap(); // Wait for sender to finish all messages

    // Lagged receiver (rx): now that all messages are sent, wait for a bit, then try to receive
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Receiver should be lagged.
    match rx.recv().await {
        Err(RecvError::Lagged(n)) => {
            // 100 sent. Cap 4. Missed 96.
            assert_eq!(
                n,
                (100 - cap) as u64,
                "Expected lagged by {} messages, got {}",
                100 - cap,
                n
            );
        }
        _ => panic!("Expected Lagged error for rx1"),
    }

    // Now, it should receive the next available message.
    let mut received_messages = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        received_messages.push(msg);
    }
    assert!(
        !received_messages.is_empty(),
        "Receiver 1 should have received some messages after lagging"
    );
    for i in 0..(received_messages.len() - 1) {
        assert!(
            received_messages[i] < received_messages[i + 1],
            "Messages not in order for Receiver 1"
        );
    }

    // Verify a non-lagged receiver (rx2) receives recent messages correctly (might also be slightly
    // lagged)
    match rx2.recv().await {
        Err(RecvError::Lagged(n)) => {
            assert_eq!(
                n,
                (100 - cap) as u64,
                "Expected lagged by {} messages, got {}",
                100 - cap,
                n
            );
        }
        _ => panic!("Expected Lagged error for rx2"),
    }

    let mut received_messages_rx2 = Vec::new();
    while let Ok(msg) = rx2.try_recv() {
        received_messages_rx2.push(msg);
    }
    assert!(
        !received_messages_rx2.is_empty(),
        "Receiver 2 should have received some messages after lagging"
    );
    for i in 0..(received_messages_rx2.len() - 1) {
        assert!(
            received_messages_rx2[i] < received_messages_rx2[i + 1],
            "Messages not in order for Receiver 2"
        );
    }
}
