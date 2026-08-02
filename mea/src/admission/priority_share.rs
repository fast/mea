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

use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use super::share::Share;

/// An admission controller that combines strict priorities with fair sharing.
///
/// Each acquisition belongs to a key and a priority. Priorities are dense
/// `usize` values starting at zero, and a larger value means a higher priority.
/// All priorities share one capacity. Configuration entries add capacity from
/// the lowest to the highest priority, so the admission limit at a priority is
/// the sum of entries up to and including that priority. Every assigned permit
/// counts toward this limit, regardless of its priority. Higher priorities can
/// therefore use reserved headroom that is unavailable to lower priorities.
///
/// When contended, queued acquisitions at a higher priority are admitted before
/// acquisitions at a lower priority. Within one priority, the key with the
/// fewest permits currently held is admitted first. Ties are resolved by queue
/// order.
///
/// Priority only affects admission. An acquisition that has already been
/// assigned a permit is never revoked for a later, higher-priority acquisition.
/// Sustained higher-priority demand can therefore starve lower priorities, and
/// headroom reserved for a higher priority can remain unused while lower work
/// waits.
///
/// A key must use one priority while it has held permits or queued
/// acquisitions. It may use another priority after all of its permits and
/// queued acquisitions are gone.
#[derive(Debug)]
pub struct PriorityShare<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    share: Share<K, S>,
}

impl<K> PriorityShare<K, RandomState>
where
    K: Eq + Hash,
{
    /// Creates a priority-share admission controller.
    ///
    /// `capacities` lists the additional shared capacity unlocked at each
    /// priority from lowest to highest. The admission limit for priority `p` is
    /// the sum of `capacities[0..=p]`. Individual entries may be zero as long as
    /// the total capacity is nonzero.
    ///
    /// # Panics
    ///
    /// Panics if `capacities` is empty, its total is zero, or its total
    /// overflows `usize`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mea::admission::PriorityShare;
    ///
    /// // Priority 0 can enter while fewer than two permits are assigned.
    /// // Priority 1 can use one additional permit of reserved headroom.
    /// let admission = PriorityShare::<String>::new([2, 1]);
    /// assert_eq!(admission.available_permits(), 3);
    /// ```
    pub fn new<C>(capacities: C) -> Self
    where
        C: AsRef<[usize]>,
    {
        Self::with_hasher(capacities, RandomState::new())
    }
}

impl<K, S> PriorityShare<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Creates a priority-share admission controller with the given hash
    /// builder.
    ///
    /// `capacities` has the same meaning as in [`Self::new`].
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::new`].
    pub fn with_hasher<C>(capacities: C, hash_builder: S) -> Self
    where
        C: AsRef<[usize]>,
    {
        let capacities = capacities.as_ref();
        assert!(
            !capacities.is_empty(),
            "PriorityShare requires at least one priority"
        );

        let mut total_permits = 0usize;
        let mut admission_limits = Vec::with_capacity(capacities.len());
        for capacity in capacities {
            total_permits = total_permits
                .checked_add(*capacity)
                .expect("PriorityShare capacity overflow");
            admission_limits.push(total_permits);
        }
        assert!(
            total_permits > 0,
            "PriorityShare requires at least one permit"
        );

        Self {
            share: Share::new(admission_limits.into_boxed_slice(), hash_builder),
        }
    }

    /// Returns the total number of permits that have not been assigned.
    ///
    /// This value can be nonzero while a lower-priority acquisition is waiting
    /// because the shared usage has reached that priority's admission limit,
    /// leaving headroom for higher priorities. A permit assigned to a queued
    /// acquisition is no longer counted even if that acquisition has not yet
    /// been polled again.
    pub fn available_permits(&self) -> usize {
        self.share.available_permits()
    }

    /// Returns the total number of acquisitions waiting for a permit.
    ///
    /// An acquisition is no longer counted once it has been assigned a permit,
    /// even if its future has not yet been polled again.
    pub fn num_waiters(&self) -> usize {
        self.share.num_waiters()
    }

    /// Attempts to acquire one permit for `key` at `priority` without waiting.
    ///
    /// This method may bypass queued acquisitions at lower priorities. It does
    /// not bypass a queued acquisition that should be admitted first at the
    /// same or a higher priority.
    ///
    /// # Panics
    ///
    /// Panics if `priority` is not configured, or if `key` already has held
    /// permits or waiters at another priority.
    pub fn try_acquire(&self, key: K, priority: usize) -> Option<PrioritySharePermit<'_, K, S>> {
        let key = Arc::new(key);
        self.share
            .try_acquire(key.clone(), priority)
            .then(|| PrioritySharePermit {
                admission: self,
                key,
                priority,
            })
    }

    /// Acquires one permit for `key` at `priority`.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::try_acquire`].
    ///
    /// # Cancel safety
    ///
    /// Cancelling this method loses the acquisition's place in the queue. If
    /// a permit has already been assigned, cancellation releases it for another
    /// queued acquisition.
    pub async fn acquire(&self, key: K, priority: usize) -> PrioritySharePermit<'_, K, S> {
        let key = Arc::new(key);
        self.share.acquire(key.clone(), priority).await;
        PrioritySharePermit {
            admission: self,
            key,
            priority,
        }
    }

    /// Attempts to acquire one owned permit for `key` at `priority` without
    /// waiting.
    ///
    /// The admission controller must be wrapped in an [`Arc`] to call this
    /// method.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::try_acquire`].
    pub fn try_acquire_owned(
        self: Arc<Self>,
        key: K,
        priority: usize,
    ) -> Option<OwnedPrioritySharePermit<K, S>> {
        let key = Arc::new(key);
        if !self.share.try_acquire(key.clone(), priority) {
            return None;
        }
        Some(OwnedPrioritySharePermit {
            admission: self,
            key,
            priority,
        })
    }

    /// Acquires one owned permit for `key` at `priority`.
    ///
    /// The admission controller must be wrapped in an [`Arc`] to call this
    /// method.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::try_acquire`].
    ///
    /// # Cancel safety
    ///
    /// This method has the same cancellation behavior as [`Self::acquire`].
    pub async fn acquire_owned(
        self: Arc<Self>,
        key: K,
        priority: usize,
    ) -> OwnedPrioritySharePermit<K, S> {
        let key = Arc::new(key);
        self.share.acquire(key.clone(), priority).await;
        OwnedPrioritySharePermit {
            admission: self,
            key,
            priority,
        }
    }

    fn release(&self, key: &K) {
        self.share.release(key);
    }
}

/// A permit from a [`PriorityShare`] admission controller.
///
/// Dropping this permit returns it to the admission controller and may admit a
/// queued acquisition.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct PrioritySharePermit<'a, K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: &'a PriorityShare<K, S>,
    key: Arc<K>,
    priority: usize,
}

impl<K, S> PrioritySharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns the priority associated with this permit.
    pub fn priority(&self) -> usize {
        self.priority
    }
}

impl<K, S> Drop for PrioritySharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.key);
    }
}

/// An owned permit from a [`PriorityShare`] admission controller.
///
/// Unlike [`PrioritySharePermit`], this type owns an [`Arc`] to the admission
/// controller and has no lifetime parameter. Dropping it returns the permit and
/// may admit a queued acquisition.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedPrioritySharePermit<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: Arc<PriorityShare<K, S>>,
    key: Arc<K>,
    priority: usize,
}

impl<K, S> OwnedPrioritySharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns the priority associated with this permit.
    pub fn priority(&self) -> usize {
        self.priority
    }
}

impl<K, S> Drop for OwnedPrioritySharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.key);
    }
}
