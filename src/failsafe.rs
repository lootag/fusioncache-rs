use chrono::{DateTime, Utc};
use moka::future::Cache;
use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailSafeResult<TValue> {
    Hit(TValue),
    Miss,
    TooManyCycles,
    CurrentCycleEnded,
    NotInFailSafeMode,
}

#[derive(Clone, Debug)]
pub(crate) struct FailSafeConfiguration {
    entry_ttl: std::time::Duration,
    failsafe_ttl: std::time::Duration,
    max_cycles: Option<u64>,
    soft_timeout: Option<std::time::Duration>,
}

impl FailSafeConfiguration {
    pub fn new(
        entry_ttl: std::time::Duration,
        failsafe_ttl: std::time::Duration,
        max_cycles: Option<u64>,
        soft_timeout: Option<std::time::Duration>,
    ) -> Self {
        Self {
            entry_ttl,
            failsafe_ttl,
            max_cycles,
            soft_timeout,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FailSafeCache<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + 'static,
> {
    configuration: FailSafeConfiguration,
    cycle_start: Arc<Mutex<HashMap<TKey, DateTime<Utc>>>>,
    current_cycle: Arc<Mutex<HashMap<TKey, u64>>>,
    cache: Cache<TKey, TValue>,
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + 'static>
    FailSafeCache<TKey, TValue>
{
    pub fn new(configuration: FailSafeConfiguration) -> Self {
        let cache = Cache::builder()
            .time_to_live(configuration.entry_ttl)
            .build();
        Self {
            configuration,
            cycle_start: Arc::new(Mutex::new(HashMap::new())),
            current_cycle: Arc::new(Mutex::new(HashMap::new())),
            cache,
        }
    }
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + 'static>
    FailSafeCache<TKey, TValue>
{
    pub async fn start_failsafe_cycle(&mut self, key: TKey) {
        let mut cycle_start = self.cycle_start.lock().await;
        let mut current_cycle = self.current_cycle.lock().await;
        cycle_start.insert(key.clone(), Utc::now());
        drop(cycle_start);
        if !current_cycle.contains_key(&key) {
            current_cycle.insert(key.clone(), 0);
        }
        drop(current_cycle);
    }

    pub async fn exit_failsafe_mode(&mut self, key: TKey) {
        let mut cycle_start = self.cycle_start.lock().await;
        let mut current_cycle = self.current_cycle.lock().await;
        cycle_start.remove(&key);
        drop(cycle_start);
        current_cycle.remove(&key);
        drop(current_cycle);
    }

    pub async fn get(&mut self, key: &TKey) -> FailSafeResult<TValue> {
        let cycle_start_map = self.cycle_start.lock().await;
        let mut current_cycle_map = self.current_cycle.lock().await;
        if let Some(cycle_start) = cycle_start_map.get(key) {
            if Utc::now() >= cycle_start.clone() + self.configuration.failsafe_ttl {
                drop(current_cycle_map);
                return FailSafeResult::CurrentCycleEnded;
            }
            drop(cycle_start_map);
            if let Some(max_cycles) = self.configuration.max_cycles {
                let current_cycle = current_cycle_map.get(key).unwrap();
                if *current_cycle >= max_cycles {
                    drop(current_cycle_map);
                    return FailSafeResult::TooManyCycles;
                } else {
                    let updated_current_cycle = *current_cycle + 1;
                    current_cycle_map.insert(key.clone(), updated_current_cycle);
                    drop(current_cycle_map);
                    if let Some(entry) = self.cache.get(key).await {
                        return FailSafeResult::Hit(entry);
                    } else {
                        return FailSafeResult::Miss;
                    }
                }
            } else {
                drop(current_cycle_map);
                if let Some(entry) = self.cache.get(key).await {
                    return FailSafeResult::Hit(entry);
                } else {
                    return FailSafeResult::Miss;
                }
            }
        } else {
            return FailSafeResult::NotInFailSafeMode;
        }
    }

    pub async fn insert(&self, key: TKey, value: TValue) {
        self.cache.insert(key, value).await;
    }

    pub fn soft_timeout(&self) -> Option<std::time::Duration> {
        self.configuration.soft_timeout
    }

    pub async fn invalidate(&self, key: &TKey) {
        self.cache.invalidate(key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fail_safe_cache_returns_not_in_fail_safe_mode_if_no_cycle_has_started() {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5),
            None,
            None,
        ));
        cache.insert(1, 1).await;
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::NotInFailSafeMode);
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_current_cycle_ended_if_the_current_cycle_has_ended() {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5),
            None,
            None,
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle(1).await;
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::CurrentCycleEnded);
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_hit_if_the_key_is_in_the_cache() {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5),
            None,
            None,
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle(1).await;
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::Hit(1));
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_miss_if_the_key_is_not_in_the_cache() {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(2),
            None,
            None,
        ));
        cache.insert(1, 1).await;
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        cache.start_failsafe_cycle(1).await;
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::Miss);
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_too_many_cycles_if_the_maximum_number_of_cycles_has_been_reached()
     {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(1),
            Some(3),
            None,
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle(1).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::Hit(1));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle(1).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::Hit(1));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle(1).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::Hit(1));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle(1).await;
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::TooManyCycles);
    }
}
