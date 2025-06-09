use async_trait::async_trait;
use failsafe::{FailSafeCache, FailSafeConfiguration, FailSafeResult};
use moka::future::Cache;
use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, broadcast},
    task,
};

mod failsafe;

#[async_trait]
pub trait Factory<
    TKey: Clone + Send + Sync + 'static,
    TValue: Clone + Send + Sync + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
>: Send + Sync + Clone + 'static
{
    async fn get(&self, key: &TKey) -> Result<TValue, TError>;
}

pub struct FusionCacheOptions {}

pub struct FusionCacheBuilder<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + 'static,
> {
    capacity: u64,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    hard_timeout: Option<std::time::Duration>,
    should_factory_execute_in_background: bool,
    phantom_t_key: PhantomData<TKey>,
    phantom_t_value: PhantomData<TValue>,
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + 'static>
    FusionCacheBuilder<TKey, TValue>
{
    pub fn new() -> Self {
        Self {
            capacity: 1000,
            fail_safe_configuration: None,
            should_factory_execute_in_background: false,
            phantom_t_key: PhantomData,
            phantom_t_value: PhantomData,
            hard_timeout: None,
        }
    }

    pub fn with_capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_fail_safe(
        mut self,
        entry_ttl: std::time::Duration,
        failsafe_ttl: std::time::Duration,
        max_cycles: Option<u64>,
        soft_timeout: Option<std::time::Duration>,
    ) -> Self {
        self.fail_safe_configuration = Some(FailSafeConfiguration::new(
            entry_ttl,
            failsafe_ttl,
            max_cycles,
            soft_timeout,
        ));
        self
    }

    pub fn with_hard_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.hard_timeout = Some(timeout);
        self
    }

    pub fn set_factory_background_execution(mut self, should_execute_in_background: bool) -> Self {
        self.should_factory_execute_in_background = should_execute_in_background;
        self
    }

    pub fn build(self) -> FusionCache<TKey, TValue> {
        FusionCache {
            cache: Cache::new(self.capacity),
            should_factory_execute_in_background: self.should_factory_execute_in_background,
            hard_timeout: self.hard_timeout,
            fail_safe_cache: self.fail_safe_configuration.map(|c| FailSafeCache::new(c)),
            in_flight_factory_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FusionCacheError {
    Other,
    SystemCorruption,
    FactoryError,
    FactoryTimeout,
}

#[derive(Clone)]
pub struct FusionCache<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + Clone + 'static,
> {
    cache: Cache<TKey, TValue>,
    hard_timeout: Option<std::time::Duration>,
    should_factory_execute_in_background: bool,
    fail_safe_cache: Option<FailSafeCache<TKey, TValue>>,
    // This map holds the in-flight factory requests for each key.
    in_flight_factory_requests:
        Arc<Mutex<HashMap<TKey, broadcast::Sender<Result<TValue, FusionCacheError>>>>>,
}

impl<TKey: Hash + Eq + Send + Sync + Clone + 'static, TValue: Clone + Send + Sync + Clone + 'static>
    FusionCache<TKey, TValue>
{
    pub async fn get_or_set<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
    ) -> Result<TValue, FusionCacheError> {
        let mut in_flight_factory_requests = self.in_flight_factory_requests.lock().await;
        let maybe_factory_sender = in_flight_factory_requests.get(&key).cloned();
        let maybe_entry = self.cache.get(&key).await;
        match maybe_entry {
            Some(entry) => Ok(entry),
            None => match maybe_factory_sender {
                Some(factory_sender) => {
                    drop(in_flight_factory_requests);
                    let mut factory_receiver = factory_sender.subscribe();
                    let factory_receiver_result = factory_receiver.recv().await;
                    if let Ok(factory_result) = factory_receiver_result {
                        factory_result.map_err(|e| e.into())
                    } else {
                        // The value might be in the cache, but the receiver could return an error because
                        // the sender was dropped. In this case, we should try to get the value from the cache.
                        self.cache
                            .get(&key)
                            .await
                            .ok_or(FusionCacheError::SystemCorruption)
                    }
                }
                None => {
                    let (factory_sender, _) = broadcast::channel(1);
                    in_flight_factory_requests.insert(key.clone(), factory_sender.clone());
                    drop(in_flight_factory_requests);
                    self.get_from_factory_or_fail_safe(key, factory, factory_sender)
                        .await
                }
            },
        }
    }

    async fn get_from_factory_or_fail_safe<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        factory_sender: broadcast::Sender<Result<TValue, FusionCacheError>>,
    ) -> Result<TValue, FusionCacheError> {
        if let Some(fail_safe_cache) = &mut self.fail_safe_cache {
            let fail_safe_result = fail_safe_cache.get(&key).await;
            match fail_safe_result {
                FailSafeResult::NotInFailSafeMode | FailSafeResult::CurrentCycleEnded => {
                    self.do_get_from_factory_or_failsafe(key, factory, factory_sender)
                        .await
                }
                FailSafeResult::Hit(value) => {
                    let _ = factory_sender.send(Ok(value.clone()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Ok(value)
                }
                FailSafeResult::Miss => {
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    let _ = factory_sender.send(Err(FusionCacheError::FactoryError.into()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Err(FusionCacheError::FactoryError)
                }
                FailSafeResult::TooManyCycles => {
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    let factory_result = factory.get(&key).await;
                    match factory_result {
                        Ok(factory_value) => {
                            let _ = factory_sender.send(Ok(factory_value.clone()));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Ok(factory_value)
                        }
                        Err(e) => {
                            let _ = factory_sender.send(Err(e.into()));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Err(FusionCacheError::FactoryError)
                        }
                    }
                }
            }
        } else {
            let factory_result = if let Some(hard_timeout) = self.hard_timeout {
                tokio::time::timeout(hard_timeout, factory.get(&key))
                    .await
                    .map_err(|_| FusionCacheError::FactoryTimeout)?
            } else {
                factory.get(&key).await
            };
            match factory_result {
                Ok(factory_value) => {
                    self.cache.insert(key.clone(), factory_value.clone()).await;
                    let _ = factory_sender.send(Ok(factory_value.clone()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Ok(factory_value)
                }
                Err(e) => {
                    let _ = factory_sender.send(Err(e.clone().into()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Err(e.into())
                }
            }
        }
    }

    async fn remove_from_in_flight_factory_requests(&mut self, key: &TKey) {
        let mut in_flight_factory_requests = self.in_flight_factory_requests.lock().await;
        if let Some(_) = in_flight_factory_requests.get(key) {
            in_flight_factory_requests.remove(key);
        }
        drop(in_flight_factory_requests);
    }

    pub async fn get(&self, key: TKey) -> Option<TValue> {
        self.cache.get(&key).await
    }

    pub async fn set(&self, key: TKey, value: TValue) {
        self.cache.insert(key, value).await;
    }

    pub async fn evict(&self, key: TKey) {
        self.cache.remove(&key).await;
    }

    // This method only gets called when the cache is created with fail-safe enabled.
    async fn do_get_from_factory_or_failsafe<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        factory_sender: broadcast::Sender<Result<TValue, FusionCacheError>>,
    ) -> Result<TValue, FusionCacheError> {
        let _key = key.clone();
        let _factory = factory.clone();
        let _factory_sender = factory_sender.clone();
        let mut _self = self.clone();

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<TValue, FusionCacheError>>();
        if let Some(soft_timeout) = self.fail_safe_cache.as_ref().and_then(|c| c.soft_timeout()) {
            let factory_task = task::spawn(async move {
                let hard_timeout = _self.hard_timeout.unwrap_or(Duration::MAX);
                let factory_result = select! {
                    result = _factory.get(&_key) => {
                        result.map_err(|e| e.into())
                    },
                    _ = tokio::time::sleep(hard_timeout) => {
                        Err(FusionCacheError::FactoryError)
                    }
                };

                match factory_result {
                    Ok(factory_value) => {
                        let _ = _factory_sender.send(Ok(factory_value.clone()));
                        _self
                            .cache
                            .insert(_key.clone(), factory_value.clone())
                            .await;

                        let fail_safe_cache = _self.fail_safe_cache.as_mut().unwrap();
                        fail_safe_cache
                            .insert(_key.clone(), factory_value.clone())
                            .await;
                        fail_safe_cache.exit_failsafe_mode(_key.clone()).await;
                        _self.remove_from_in_flight_factory_requests(&_key).await;
                        let _ = tx.send(Ok(factory_value));
                    }
                    Err(e) => {
                        let _ = _factory_sender.send(Err(e.clone().into()));
                        _self.remove_from_in_flight_factory_requests(&_key).await;
                        let _ = tx.send(Err(e.into()));
                    }
                };
            });
            select! {
                factory_result = rx => {
                    if let Ok(r) = factory_result {
                        r.map_err(|e| e.into())
                    } else {
                        Err(FusionCacheError::SystemCorruption)
                    }
                }
                _ = tokio::time::sleep(soft_timeout) => {
                    let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                    fail_safe_cache.start_failsafe_cycle(key.clone()).await;
                    let fail_safe_result = fail_safe_cache.get(&key).await;
                    match fail_safe_result {
                        FailSafeResult::Hit(value) => {
                            let _ = factory_sender.send(Ok(value.clone()));
                            if !self.should_factory_execute_in_background {
                                factory_task.abort();
                                self.remove_from_in_flight_factory_requests(&key).await;
                            }
                            Ok(value)
                        }
                        FailSafeResult::Miss => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            if let Some(hard_timeout) = self.hard_timeout {
                                tokio::time::sleep(hard_timeout - soft_timeout).await;
                                factory_task.abort();
                                self.remove_from_in_flight_factory_requests(&key).await;
                                Err(FusionCacheError::FactoryTimeout)
                            } else {
                                Err(FusionCacheError::FactoryTimeout)
                            }
                        }
                        FailSafeResult::TooManyCycles => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            if let Some(hard_timeout) = self.hard_timeout {
                                tokio::time::sleep(hard_timeout - soft_timeout).await;
                                factory_task.abort();
                                self.remove_from_in_flight_factory_requests(&key).await;
                                Err(FusionCacheError::FactoryTimeout)
                            } else {
                                Err(FusionCacheError::FactoryTimeout)
                            }
                        }
                        FailSafeResult::CurrentCycleEnded => {
                            panic!(
                                "Got FailSafeResult::CurrentCycleEnded, but we just started a cycle"
                            );
                        }
                        FailSafeResult::NotInFailSafeMode => {
                            panic!(
                                "Got FailSafeResult::NotInFailSafeMode, but we just entered fail safe mode"
                            );
                        }
                    }
                }
            }
        } else if let Some(hard_timeout) = self.hard_timeout {
            let result = select! {
                factory_result = factory.get(&key) => {
                    factory_result.map_err(|e| e.into())
                }
                _ = tokio::time::sleep(hard_timeout) => {
                    Err(FusionCacheError::FactoryTimeout)
                }
            };

            match result {
                Ok(factory_value) => {
                    // I can unwrap safely because I already know that fail_safe_cache is Some
                    let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                    self.cache.insert(key.clone(), factory_value.clone()).await;

                    fail_safe_cache
                        .insert(key.clone(), factory_value.clone())
                        .await;
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    let _ = factory_sender.send(Ok(factory_value.clone()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Ok(factory_value)
                }
                Err(e) => {
                    let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                    fail_safe_cache.start_failsafe_cycle(key.clone()).await;
                    let fail_safe_result = fail_safe_cache.get(&key).await;
                    match fail_safe_result {
                        FailSafeResult::Hit(value) => {
                            let _ = factory_sender.send(Ok(value.clone()));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Ok(value)
                        }
                        FailSafeResult::Miss => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            let _ = factory_sender.send(Err(e));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Err(FusionCacheError::FactoryError)
                        }
                        FailSafeResult::TooManyCycles => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            let _ = factory_sender.send(Err(e));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Err(FusionCacheError::FactoryError)
                        }
                        FailSafeResult::CurrentCycleEnded => {
                            panic!(
                                "Got FailSafeResult::CurrentCycleEnded, but we just started a cycle"
                            );
                        }
                        FailSafeResult::NotInFailSafeMode => {
                            panic!(
                                "Got FailSafeResult::NotInFailSafeMode, but we just entered fail safe mode"
                            );
                        }
                    }
                }
            }
        } else {
            let result = factory.get(&key).await.map_err(|e| e.into());

            match result {
                Ok(factory_value) => {
                    // I can unwrap safely because I already know that fail_safe_cache is Some
                    let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                    self.cache.insert(key.clone(), factory_value.clone()).await;

                    fail_safe_cache
                        .insert(key.clone(), factory_value.clone())
                        .await;
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    let _ = factory_sender.send(Ok(factory_value.clone()));
                    self.remove_from_in_flight_factory_requests(&key).await;
                    Ok(factory_value)
                }
                Err(e) => {
                    let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                    fail_safe_cache.start_failsafe_cycle(key.clone()).await;
                    let fail_safe_result = fail_safe_cache.get(&key).await;
                    match fail_safe_result {
                        FailSafeResult::Hit(value) => {
                            let _ = factory_sender.send(Ok(value.clone()));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Ok(value)
                        }
                        FailSafeResult::Miss => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            let _ = factory_sender.send(Err(e));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Err(FusionCacheError::FactoryError)
                        }
                        FailSafeResult::TooManyCycles => {
                            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                            let _ = factory_sender.send(Err(e));
                            self.remove_from_in_flight_factory_requests(&key).await;
                            Err(FusionCacheError::FactoryError)
                        }
                        FailSafeResult::CurrentCycleEnded => {
                            panic!(
                                "Got FailSafeResult::CurrentCycleEnded, but we just started a cycle"
                            );
                        }
                        FailSafeResult::NotInFailSafeMode => {
                            panic!(
                                "Got FailSafeResult::NotInFailSafeMode, but we just entered fail safe mode"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use futures::future::join_all;

    #[tokio::test]
    async fn test_factory_only_gets_called_once_if_multiple_threads_request_the_same_key() {
        let factory = TestFactory::new();
        let cache = FusionCacheBuilder::new().build();
        let key = 1;
        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(key, factory.clone()).await.unwrap();
                assert_eq!(value, 1);
            }));
        }
        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_failsafe_hits_when_there_is_an_entry_in_failsafe_cache() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(10),
                Duration::from_secs(5),
                Some(3),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let failsafe_value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, failsafe_value.unwrap())
    }

    #[tokio::test]
    async fn test_failsafe_hits_when_there_is_an_entry_in_failsafe_cache_multithreaded() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(10),
                Duration::from_secs(5),
                Some(3),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone()).await.unwrap();
                assert_eq!(value, 1);
            }));
        }
        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_failsafe_misses_when_there_is_no_entry_in_failsafe_cache() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(2),
                Duration::from_secs(1),
                Some(3),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone()).await;
        assert!(failsafe_value.is_err());
    }

    #[tokio::test]
    async fn test_failsafe_misses_when_there_is_no_entry_in_failsafe_cache_multithreaded() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(2),
                Duration::from_secs(1),
                Some(3),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone()).await;
                assert!(value.is_err());
            }));
        }
        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_failsafe_misses_when_the_maximum_number_of_cycles_is_reached() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(15),
                Duration::from_secs(1),
                Some(2),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let failsafe_value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, failsafe_value.unwrap());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, failsafe_value.unwrap());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone()).await;
        assert!(failsafe_value.is_err());
    }

    #[tokio::test]
    async fn test_failsafe_misses_when_the_maximum_number_of_cycles_is_reached_multithreaded() {
        let factory = FallibleTestFactory::new();
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(15),
                Duration::from_secs(1),
                Some(2),
                None,
            )
            .build();

        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone()).await;
                assert_eq!(1, value.unwrap());
            }));
        }
        join_all(handles).await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone()).await;
                assert_eq!(1, value.unwrap());
            }));
        }
        join_all(handles).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone()).await;
                assert!(value.is_err());
            }));
        }
        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_soft_timeout_with_failsafe_hit() {
        let factory = SlowTestFactory::new(Duration::from_secs(5), 1);
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(10),      // entry ttl
                Duration::from_secs(5),       // failsafe ttl
                Some(3),                      // max cycles
                Some(Duration::from_secs(1)), // soft timeout
            )
            .build();

        // First call to populate both caches
        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        // Second call should hit failsafe before factory completes
        let start = std::time::Instant::now();
        let value = cache.get_or_set(1, factory.clone()).await;
        let elapsed = start.elapsed();

        assert_eq!(1, value.unwrap());
        assert!(
            elapsed < Duration::from_secs(5),
            "Should return before factory completes"
        );
    }

    #[tokio::test]
    async fn test_hard_timeout_triggers() {
        let factory = SlowTestFactory::new(Duration::from_secs(5), 0);
        let mut cache = FusionCacheBuilder::new()
            .with_hard_timeout(Duration::from_secs(2))
            .build();

        let result = cache.get_or_set(1, factory).await;
        assert!(matches!(result, Err(FusionCacheError::FactoryTimeout)));
    }

    #[tokio::test]
    async fn test_background_execution_continues_after_failsafe() {
        let factory = SlowTestFactory::new(Duration::from_secs(3), 1);
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(10),
                Duration::from_secs(5),
                Some(3),
                Some(Duration::from_secs(1)),
            )
            .set_factory_background_execution(true)
            .build();

        // Populate the caches
        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        // Trigger failsafe hit and background execution
        cache.evict(1).await;
        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        // Wait for background execution to complete
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Value should be updated in main cache
        let cached_value = cache.get(1).await;
        assert_eq!(
            Some(2),
            cached_value,
            "Cache should be updated by background task"
        );
    }

    #[tokio::test]
    async fn test_background_execution_does_not_continue_after_failsafe() {
        let factory = SlowTestFactory::new(Duration::from_secs(3), 1);
        let mut cache = FusionCacheBuilder::new()
            .with_fail_safe(
                Duration::from_secs(10),
                Duration::from_secs(5),
                Some(3),
                Some(Duration::from_secs(1)),
            )
            .set_factory_background_execution(false)
            .build();

        // Populate the caches
        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        // Trigger failsafe hit and background execution
        cache.evict(1).await;
        let value = cache.get_or_set(1, factory.clone()).await;
        assert_eq!(1, value.unwrap());

        // Wait for background execution to complete
        tokio::time::sleep(Duration::from_secs(4)).await;

        // The value should not be in the main cache.
        let cached_value = cache.get(1).await;
        assert_eq!(
            None, cached_value,
            "Cache should not be updated by background task"
        );
    }

    #[derive(Clone)]
    struct SlowTestFactory {
        counter: Arc<Mutex<u32>>,
        delay: Duration,
        delay_after_n_invocations: u32,
    }

    impl SlowTestFactory {
        fn new(delay: Duration, delay_after_n_invocations: u32) -> Self {
            Self {
                counter: Arc::new(Mutex::new(0)),
                delay_after_n_invocations,
                delay,
            }
        }
    }

    #[async_trait]
    impl Factory<u32, u32, TestFactoryError> for SlowTestFactory {
        async fn get(&self, _: &u32) -> Result<u32, TestFactoryError> {
            let mut counter = self.counter.lock().await;
            if *counter >= self.delay_after_n_invocations {
                tokio::time::sleep(self.delay).await;
            }
            *counter += 1;
            Ok(*counter)
        }
    }

    #[derive(Clone)]
    struct TestFactory {
        counter: Arc<Mutex<u32>>,
    }

    impl TestFactory {
        fn new() -> Self {
            Self {
                counter: Arc::new(Mutex::new(0)),
            }
        }
    }

    #[async_trait]
    impl Factory<u32, u32, TestFactoryError> for TestFactory {
        async fn get(&self, _: &u32) -> Result<u32, TestFactoryError> {
            let mut counter = self.counter.lock().await;
            tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
            *counter += 1;
            Ok(*counter)
        }
    }

    #[derive(Clone)]
    struct TestFactoryError;

    impl Into<FusionCacheError> for TestFactoryError {
        fn into(self) -> FusionCacheError {
            FusionCacheError::Other
        }
    }

    #[derive(Clone)]
    struct FallibleTestFactory {
        counter: Arc<Mutex<u32>>,
    }

    impl FallibleTestFactory {
        pub fn new() -> Self {
            Self {
                counter: Arc::new(Mutex::new(0)),
            }
        }
    }

    #[async_trait]
    impl Factory<u32, u32, TestFactoryError> for FallibleTestFactory {
        async fn get(&self, _: &u32) -> Result<u32, TestFactoryError> {
            let mut counter = self.counter.lock().await;
            *counter += 1;
            if *counter == 1 {
                Ok(1)
            } else {
                Err(TestFactoryError)
            }
        }
    }
}
