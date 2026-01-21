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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use crate::internal::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
#[derive(Debug)]
pub struct OnceMap<K, V, S = RandomState> {
    map: Mutex<HashMap<K, Arc<OnceCell<V>>, S>>,
}

impl<K, V, S> Default for OnceMap<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher + Clone + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V> OnceMap<K, V, RandomState>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new OnceMap with the default hasher.
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher + Clone,
{
    /// Creates a new OnceMap with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            map: Mutex::new(HashMap::with_hasher(hasher)),
        }
    }

    /// Create a OnceMap with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            map: Mutex::new(HashMap::with_capacity_and_hasher(capacity, hasher)),
        }
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    pub async fn compute<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        // 1. Get or create the OnceCell.
        let cell = {
            let mut map = self.map.lock();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        // 2. Try to initialize the cell.
        // OnceCell::get_or_init guarantees that only one task executes the closure.
        let res = cell.get_or_init(func).await;
        res.clone()
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    ///
    /// If the computation fails, the error is returned and the value is not stored. Other tasks
    /// waiting for the value will retry the computation.
    pub async fn try_compute<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        // 1. Get or create the OnceCell.
        let cell = {
            let mut map = self.map.lock();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        // 2. Try to initialize the cell.
        // OnceCell::get_or_try_init guarantees that only one task executes the closure.
        let res = cell.get_or_try_init(func).await?;
        Ok(res.clone())
    }

    /// Get a clone of the value for the given key if exists.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let map = self.map.lock();
        let cell = map.get(key)?;
        cell.get().cloned()
    }

    /// Remove the given key from the map.
    ///
    /// If you need to get the value that has been remove, use the [`remove`] method instead.
    ///
    /// [`remove`]: Self::remove
    pub fn forget<Q: ?Sized>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut map = self.map.lock();
        map.remove(key);
    }

    /// Remove the given key from the map and return a *clone* of the value if exists.
    ///
    /// If you do not need to get the value that has been removed, use the [`forget`] method
    /// instead.
    ///
    /// [`forget`]: Self::forget
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let cell = self.map.lock().remove(key)?;
        cell.get().cloned()
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: Default + BuildHasher + Clone,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            map: Mutex::new(
                iter.into_iter()
                    .map(|(k, v)| (k, Arc::new(OnceCell::from_value(v))))
                    .collect(),
            ),
        }
    }
}
