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

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::FairShare;
use super::PriorityShare;

fn poll_once<F>(future: Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
#[should_panic(expected = "PriorityShare requires at least one priority")]
fn empty_capacities_panic() {
    PriorityShare::<usize>::new([]);
}

#[test]
#[should_panic(expected = "PriorityShare requires at least one permit")]
fn zero_total_capacity_panics() {
    PriorityShare::<usize>::new([0, 0]);
}

#[test]
#[should_panic(expected = "PriorityShare capacity overflow")]
fn capacity_overflow_panics() {
    PriorityShare::<usize>::new([usize::MAX, 1]);
}

#[test]
#[should_panic(expected = "outside the configured priority range")]
fn invalid_priority_panics() {
    let admission = PriorityShare::new([1]);
    let _ = admission.try_acquire("owner", 1);
}

#[test]
fn all_priorities_count_toward_shared_admission_thresholds() {
    let admission = PriorityShare::new([4, 1]);
    let low0 = admission.try_acquire("low-0", 0).unwrap();
    let low1 = admission.try_acquire("low-1", 0).unwrap();
    let low2 = admission.try_acquire("low-2", 0).unwrap();
    let high0 = admission.try_acquire("high-0", 1).unwrap();

    assert_eq!(admission.available_permits(), 1);
    assert!(admission.try_acquire("low-3", 0).is_none());
    assert_eq!(admission.available_permits(), 1);

    let high1 = admission.try_acquire("high-1", 1).unwrap();
    assert_eq!(high1.priority(), 1);
    assert_eq!(admission.available_permits(), 0);
    assert!(admission.try_acquire("high-2", 1).is_none());

    drop((low0, low1, low2, high0, high1));
    assert_eq!(admission.available_permits(), 5);
}

#[test]
fn higher_priority_can_use_entire_shared_capacity() {
    let admission = PriorityShare::new([2, 1]);
    let permits = [
        admission.try_acquire("high-0", 1).unwrap(),
        admission.try_acquire("high-1", 1).unwrap(),
        admission.try_acquire("high-2", 1).unwrap(),
    ];

    assert_eq!(admission.available_permits(), 0);
    assert!(admission.try_acquire("high-3", 1).is_none());

    drop(permits);
    assert_eq!(admission.available_permits(), 3);
}

#[test]
fn higher_priority_bypasses_queued_lower_priority() {
    let admission = PriorityShare::new([1, 1]);
    let low = admission.try_acquire("low-held", 0).unwrap();

    let low_waiter = admission.acquire("low-waiter", 0);
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    assert_eq!(admission.available_permits(), 1);
    assert_eq!(admission.num_waiters(), 1);

    let high = admission.try_acquire("high", 1).unwrap();
    assert_eq!(high.key(), &"high");
    assert_eq!(high.priority(), 1);
    assert_eq!(admission.num_waiters(), 1);

    drop(low);
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    assert_eq!(admission.available_permits(), 1);

    drop(high);
    let low_waiter = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => {
            panic!("the lower-priority waiter should use shared capacity below its threshold")
        }
    };

    drop(low_waiter);
}

#[test]
fn released_capacity_goes_to_the_highest_priority_waiter() {
    let admission = PriorityShare::new([1, 0]);
    let held = admission.try_acquire("held", 0).unwrap();

    let low = admission.acquire("low", 0);
    let mut low = pin!(low);
    assert!(poll_once(low.as_mut()).is_pending());

    let high = admission.acquire("high", 1);
    let mut high = pin!(high);
    assert!(poll_once(high.as_mut()).is_pending());

    drop(held);
    assert!(poll_once(low.as_mut()).is_pending());
    let high_permit = match poll_once(high.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the higher-priority waiter should be admitted first"),
    };

    drop(high_permit);
    let low_permit = match poll_once(low.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the lower-priority waiter should eventually be admitted"),
    };
    drop(low_permit);
}

#[test]
fn assigned_permit_is_not_revoked_for_a_higher_priority_waiter() {
    let admission = PriorityShare::new([1, 0]);
    let held = admission.try_acquire("held", 0).unwrap();

    let low = admission.acquire("low", 0);
    let mut low = pin!(low);
    assert!(poll_once(low.as_mut()).is_pending());
    drop(held);
    assert_eq!(admission.num_waiters(), 0);

    let high = admission.acquire("high", 1);
    let mut high = pin!(high);
    assert!(poll_once(high.as_mut()).is_pending());

    let low_permit = match poll_once(low.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("an assigned lower-priority permit must not be revoked"),
    };
    assert!(poll_once(high.as_mut()).is_pending());

    drop(low_permit);
    let high_permit = match poll_once(high.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the higher-priority waiter should receive the released permit"),
    };
    drop(high_permit);
}

#[test]
fn cancelling_an_assigned_permit_reassigns_it_by_priority() {
    let admission = PriorityShare::new([1, 0]);
    let held = admission.try_acquire("held", 0).unwrap();

    let mut low = Box::pin(admission.acquire("low", 0));
    assert!(poll_once(low.as_mut()).is_pending());
    drop(held);

    let high = admission.acquire("high", 1);
    let mut high = pin!(high);
    assert!(poll_once(high.as_mut()).is_pending());

    drop(low);
    let high_permit = match poll_once(high.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancellation should reassign the permit by priority"),
    };
    drop(high_permit);
}

#[test]
fn fair_shares_within_one_priority() {
    let admission = PriorityShare::new([0, 3]);
    let mut held_by_a = vec![
        admission.try_acquire("a", 1).unwrap(),
        admission.try_acquire("a", 1).unwrap(),
        admission.try_acquire("a", 1).unwrap(),
    ];

    let acquire_a = admission.acquire("a", 1);
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    let acquire_b = admission.acquire("b", 1);
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());

    let acquire_c = admission.acquire("c", 1);
    let mut acquire_c = pin!(acquire_c);
    assert!(poll_once(acquire_c.as_mut()).is_pending());

    drop(held_by_a.pop().unwrap());
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner b should receive the first released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_c = match poll_once(acquire_c.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner c should receive the second released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner a should receive the third released permit"),
    };
    drop((permit_a, permit_b, permit_c));
}

#[test]
#[should_panic(expected = "an owner cannot change priority")]
fn active_owner_cannot_change_priority() {
    let admission = PriorityShare::new([1, 1]);
    let _held = admission.try_acquire("owner", 0).unwrap();
    let _ = admission.try_acquire("owner", 1);
}

#[test]
fn inactive_owner_can_change_priority() {
    let admission = PriorityShare::new([1, 1]);
    let low = admission.try_acquire("owner", 0).unwrap();
    drop(low);

    let high = admission.try_acquire("owner", 1).unwrap();
    assert_eq!(high.priority(), 1);
    drop(high);
}

#[test]
fn cancelling_higher_priority_waiter_unblocks_lower_priority() {
    let admission = PriorityShare::new([1, 0]);
    let held = admission.try_acquire("held", 0).unwrap();

    let low = admission.acquire("low", 0);
    let mut low = pin!(low);
    assert!(poll_once(low.as_mut()).is_pending());

    {
        let high = admission.acquire("high", 1);
        let mut high = pin!(high);
        assert!(poll_once(high.as_mut()).is_pending());
    }

    drop(held);
    let low_permit = match poll_once(low.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancelling the higher-priority waiter should remove it"),
    };
    drop(low_permit);
}

#[test]
fn single_priority_matches_fair_share() {
    let fair = FairShare::new(2);
    let priority = PriorityShare::new([2]);

    let fair_a0 = fair.try_acquire("a").unwrap();
    let fair_a1 = fair.try_acquire("a").unwrap();
    let priority_a0 = priority.try_acquire("a", 0).unwrap();
    let priority_a1 = priority.try_acquire("a", 0).unwrap();

    let fair_a = fair.acquire("a");
    let mut fair_a = pin!(fair_a);
    let priority_a = priority.acquire("a", 0);
    let mut priority_a = pin!(priority_a);
    assert_eq!(
        poll_once(fair_a.as_mut()).is_pending(),
        poll_once(priority_a.as_mut()).is_pending()
    );

    let fair_b = fair.acquire("b");
    let mut fair_b = pin!(fair_b);
    let priority_b = priority.acquire("b", 0);
    let mut priority_b = pin!(priority_b);
    assert_eq!(
        poll_once(fair_b.as_mut()).is_pending(),
        poll_once(priority_b.as_mut()).is_pending()
    );

    drop((fair_a0, priority_a0));
    assert!(poll_once(fair_a.as_mut()).is_pending());
    assert!(poll_once(priority_a.as_mut()).is_pending());
    let fair_b_permit = poll_once(fair_b.as_mut());
    let priority_b_permit = poll_once(priority_b.as_mut());
    assert!(fair_b_permit.is_ready());
    assert!(priority_b_permit.is_ready());

    drop((fair_b_permit, priority_b_permit));
    drop((fair_a1, priority_a1));
    assert!(poll_once(fair_a.as_mut()).is_ready());
    assert!(poll_once(priority_a.as_mut()).is_ready());
    assert_eq!(fair.available_permits(), priority.available_permits());
    assert_eq!(fair.num_waiters(), priority.num_waiters());
}

#[test]
fn supports_a_custom_hash_builder() {
    let admission = PriorityShare::<String, BuildHasherDefault<DefaultHasher>>::with_hasher(
        [1],
        BuildHasherDefault::default(),
    );
    let permit = admission.try_acquire("tenant".to_owned(), 0).unwrap();
    assert_eq!(permit.key(), "tenant");
}

#[test]
fn owned_permit_keeps_the_admission_controller_alive() {
    let admission = Arc::new(PriorityShare::new([1]));
    let permit = admission
        .clone()
        .try_acquire_owned("tenant", 0)
        .expect("a permit should be available");

    drop(admission);
    assert_eq!(permit.key(), &"tenant");
    assert_eq!(permit.priority(), 0);
    drop(permit);
}

#[test]
fn acquire_futures_are_send() {
    fn assert_send<T: Send>(_: T) {}

    let admission = PriorityShare::<String>::new([1]);
    assert_send(admission.acquire("tenant".to_owned(), 0));

    let admission = Arc::new(PriorityShare::<String>::new([1]));
    assert_send(admission.acquire_owned("tenant".to_owned(), 0));
}

#[test]
fn deterministic_events_match_capacity_model() {
    for capacities in [&[2, 1, 1][..], &[0, 2, 0, 1], &[1, 0, 2, 0]] {
        let admission = PriorityShare::new(capacities);
        let mut held = Vec::new();
        let mut seed = 0x4d59_5df4_d0f3_3173u64;
        let total_capacity = capacities.iter().sum::<usize>();

        for key in 0..2_000usize {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);

            if !held.is_empty() && seed % 3 == 0 {
                let index = seed as usize % held.len();
                drop(held.swap_remove(index));
            } else {
                let priority = seed as usize % capacities.len();
                let expected = can_admit(capacities, held.len(), priority);
                let permit = admission.try_acquire(key, priority);
                assert_eq!(permit.is_some(), expected);
                if let Some(permit) = permit {
                    held.push(permit);
                }
            }

            assert_eq!(admission.available_permits(), total_capacity - held.len());
        }
    }
}

fn can_admit(capacities: &[usize], held: usize, priority: usize) -> bool {
    held < capacities[..=priority].iter().sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_test_preserves_shared_capacity_limit() {
    const CAPACITIES: [usize; 3] = [2, 2, 1];

    let admission = Arc::new(PriorityShare::new(CAPACITIES));
    let total_capacity = CAPACITIES.iter().sum::<usize>();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for priority in 0..CAPACITIES.len() {
        for owner in 0..8usize {
            for _ in 0..16usize {
                let admission = admission.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                handles.push(tokio::spawn(async move {
                    let key = (priority, owner);
                    let _permit = admission.acquire_owned(key, priority).await;
                    let active_count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!(active_count <= total_capacity);
                    max_active.fetch_max(active_count, Ordering::SeqCst);

                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                }));
            }
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(max_active.load(Ordering::SeqCst) <= total_capacity);
    assert_eq!(admission.available_permits(), total_capacity);
}
