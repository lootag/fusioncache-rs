use chrono::{DateTime, Utc};
use moka::future::Cache;
use std::hash::Hash;

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
        Self {
            configuration,
            cycle_start: None,
            cache: Cache::new(1000),
        }
    }
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + 'static>
    FailSafeCache<TKey, TValue>
{
    pub fn start_cycle(&mut self) {
        self.cycle_start = Some(Utc::now());
    }

    pub fn close(&mut self) {
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
