use chrono::{DateTime, Utc};
use moka::future::Cache;
use std::hash::Hash;

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
    current_cycle: u64,
    max_cycles: Option<u64>,
}

impl FailSafeConfiguration {
    pub fn new(
        entry_ttl: std::time::Duration,
        failsafe_ttl: std::time::Duration,
        max_cycles: Option<u64>,
    ) -> Self {
        Self {
            entry_ttl,
            failsafe_ttl,
            current_cycle: 0,
            max_cycles,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FailSafeCache<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + 'static,
> {
    configuration: FailSafeConfiguration,
    cycle_start: Option<DateTime<Utc>>,
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
            cycle_start: None,
            cache,
        }
    }
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + 'static>
    FailSafeCache<TKey, TValue>
{
    pub fn start_failsafe_cycle(&mut self) {
        self.cycle_start = Some(Utc::now());
    }

    pub fn exit_failsafe_mode(&mut self) {
        self.cycle_start = None;
        self.configuration.current_cycle = 0;
    }

    pub async fn get(&mut self, key: &TKey) -> FailSafeResult<TValue> {
        if let Some(cycle_start) = self.cycle_start {
            if Utc::now() >= cycle_start + self.configuration.failsafe_ttl {
                return FailSafeResult::CurrentCycleEnded;
            }
            if let Some(max_cycles) = self.configuration.max_cycles {
                if self.configuration.current_cycle >= max_cycles {
                    return FailSafeResult::TooManyCycles;
                } else {
                    self.configuration.current_cycle += 1;
                    if let Some(entry) = self.cache.get(key).await {
                        return FailSafeResult::Hit(entry);
                    } else {
                        return FailSafeResult::Miss;
                    }
                }
            } else {
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
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle();
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
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle();
        let result = cache.get(&1).await;
        assert_eq!(result, FailSafeResult::Hit(1));
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_miss_if_the_key_is_not_in_the_cache() {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5),
            None,
        ));
        cache.insert(1, 1).await;
        cache.start_failsafe_cycle();
        let result = cache.get(&2).await;
        assert_eq!(result, FailSafeResult::Miss);
    }

    #[tokio::test]
    async fn test_fail_safe_cache_returns_too_many_cycles_if_the_maximum_number_of_cycles_has_been_reached()
     {
        let mut cache = FailSafeCache::new(FailSafeConfiguration::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(1),
            Some(3),
        ));
        cache.start_failsafe_cycle();
        cache.insert(1, 1).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::Hit(1));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&1).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle();
        cache.insert(2, 2).await;
        assert_eq!(cache.get(&2).await, FailSafeResult::Hit(2));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&2).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle();
        cache.insert(3, 3).await;
        assert_eq!(cache.get(&3).await, FailSafeResult::Hit(3));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(cache.get(&3).await, FailSafeResult::CurrentCycleEnded);
        cache.start_failsafe_cycle();
        let result = cache.get(&3).await;
        assert_eq!(result, FailSafeResult::TooManyCycles);
    }
}
