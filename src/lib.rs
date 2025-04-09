use failsafe::{FailSafeCache, FailSafeConfiguration, FailSafeResult};
use moka::future::Cache;
use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc};
use tokio::sync::{Mutex, broadcast};

mod failsafe;

pub trait Factory<
    TKey: Clone + Send + Sync + 'static,
    TValue: Clone + Send + Sync + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
>: Send + Sync + 'static
{
    async fn get(&self, key: &TKey) -> Result<TValue, TError>;
}

pub struct FusionCacheBuilder<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
> {
    capacity: u64,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    phantom_t_key: PhantomData<TKey>,
    phantom_t_value: PhantomData<TValue>,
    phantom_t_error: PhantomData<TError>,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
> FusionCacheBuilder<TKey, TValue, TError>
{
    pub fn new() -> Self {
        Self {
            capacity: 1000,
            fail_safe_configuration: None,
            phantom_t_key: PhantomData,
            phantom_t_value: PhantomData,
            phantom_t_error: PhantomData,
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
    ) -> Self {
        self.fail_safe_configuration = Some(FailSafeConfiguration::new(
            entry_ttl,
            failsafe_ttl,
            max_cycles,
        ));
        self
    }

    pub fn build<F: Factory<TKey, TValue, TError>>(
        self,
        factory: F,
    ) -> FusionCache<TKey, TValue, TError, F> {
        FusionCache {
            cache: Cache::new(self.capacity),
            fail_safe_cache: self.fail_safe_configuration.map(|c| FailSafeCache::new(c)),
            factory,
            in_flight_factory_requests: Arc::new(Mutex::new(HashMap::new())),
            phantom_t_error: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FusionCacheError {
    Other,
    SystemCorruption,
    FactoryError,
}

#[derive(Clone)]
pub struct FusionCache<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + Clone + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
    F: Factory<TKey, TValue, TError>,
> {
    cache: Cache<TKey, TValue>,
    fail_safe_cache: Option<FailSafeCache<TKey, TValue>>,
    factory: F,
    in_flight_factory_requests:
        Arc<Mutex<HashMap<TKey, broadcast::Sender<Result<TValue, FusionCacheError>>>>>,
    phantom_t_error: PhantomData<TError>,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + Clone + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
    F: Factory<TKey, TValue, TError>,
> FusionCache<TKey, TValue, TError, F>
{
    pub async fn get_or_set(&mut self, key: TKey) -> Result<TValue, FusionCacheError> {
        let mut in_flight_factory_requests = self.in_flight_factory_requests.lock().await;
        let maybe_factory_sender = in_flight_factory_requests.get(&key).cloned();
        let maybe_entry = self.cache.get(&key).await;
        match maybe_entry {
            Some(entry) => {
                if let Some(_) = maybe_factory_sender {
                    in_flight_factory_requests.remove(&key);
                }
                drop(in_flight_factory_requests);
                Ok(entry)
            }
            None => match maybe_factory_sender {
                Some(factory_sender) => {
                    drop(in_flight_factory_requests);
                    let mut factory_receiver = factory_sender.subscribe();
                    let factory_receiver_result = factory_receiver.recv().await;
                    if let Ok(factory_result) = factory_receiver_result {
                        factory_result.map_err(|e| e.into())
                    } else {
                        self.cache
                            .get(&key)
                            .await
                            .ok_or(FusionCacheError::SystemCorruption)
                    }
                }
                None => {
                    let (tx, _) = broadcast::channel(1);
                    in_flight_factory_requests.insert(key.clone(), tx.clone());
                    drop(in_flight_factory_requests);
                    self.get_from_factory_or_fail_safe(key, tx).await
                }
            },
        }
    }

    async fn get_from_factory_or_fail_safe(
        &mut self,
        key: TKey,
        factory_sender: broadcast::Sender<Result<TValue, FusionCacheError>>,
    ) -> Result<TValue, FusionCacheError> {
        if let Some(fail_safe_cache) = &mut self.fail_safe_cache {
            let fail_safe_result = fail_safe_cache.get(&key).await;
            match fail_safe_result {
                FailSafeResult::NotInFailSafeMode | FailSafeResult::CurrentCycleEnded => {
                    self.get_from_factory(key, factory_sender).await
                }
                FailSafeResult::Hit(value) => {
                    let _ = factory_sender.send(Ok(value.clone()));
                    Ok(value)
                }
                FailSafeResult::Miss => {
                    fail_safe_cache.exit_failsafe_mode();
                    let _ = factory_sender.send(Err(FusionCacheError::FactoryError.into()));
                    Err(FusionCacheError::FactoryError)
                }
                FailSafeResult::TooManyCycles => {
                    fail_safe_cache.exit_failsafe_mode();
                    let _ = factory_sender.send(Err(FusionCacheError::FactoryError.into()));
                    Err(FusionCacheError::FactoryError)
                }
            }
        } else {
            let factory_result = self.factory.get(&key).await;
            match factory_result {
                Ok(factory_value) => {
                    self.cache.insert(key.clone(), factory_value.clone()).await;
                    let _ = factory_sender.send(Ok(factory_value.clone()));
                    Ok(factory_value)
                }
                Err(e) => {
                    let _ = factory_sender.send(Err(e.clone().into()));
                    Err(e.into())
                }
            }
        }
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

    async fn get_from_factory(
        &mut self,
        key: TKey,
        factory_sender: broadcast::Sender<Result<TValue, FusionCacheError>>,
    ) -> Result<TValue, FusionCacheError> {
        let factory_result = self.factory.get(&key).await;
        match factory_result {
            Ok(factory_value) => {
                self.cache.insert(key.clone(), factory_value.clone()).await;

                // I can unwrap safely because I already know that fail_safe_cache is Some
                self.fail_safe_cache
                    .as_mut()
                    .unwrap()
                    .insert(key, factory_value.clone())
                    .await;
                self.fail_safe_cache.as_mut().unwrap().exit_failsafe_mode();
                let _ = factory_sender.send(Ok(factory_value.clone()));
                Ok(factory_value)
            }
            Err(e) => {
                let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                fail_safe_cache.start_failsafe_cycle();
                let fail_safe_result = fail_safe_cache.get(&key).await;
                match fail_safe_result {
                    FailSafeResult::Hit(value) => Ok(value),
                    FailSafeResult::Miss => {
                        fail_safe_cache.exit_failsafe_mode();
                        let _ = factory_sender.send(Err(e.into()));
                        Err(FusionCacheError::FactoryError)
                    }
                    FailSafeResult::TooManyCycles => {
                        panic!(
                            "Got FailSafeResult::TooManyCycles, but we just started the first cycle"
                        );
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

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future::join_all;

    #[tokio::test]
    async fn test_factory_only_gets_called_once_if_multiple_threads_request_the_same_key() {
        let cache = FusionCacheBuilder::new().build(TestFactory::new());
        let key = 1;
        let mut handles = vec![];
        for _ in 0..100000 {
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(key).await.unwrap();
                assert_eq!(value, 1);
            }));
        }
        join_all(handles).await;
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

    impl Factory<u32, u32, TestFactoryError> for TestFactory {
        async fn get(&self, key: &u32) -> Result<u32, TestFactoryError> {
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
}
