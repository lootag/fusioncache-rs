use async_trait::async_trait;
use distributed_cache::{DistributedCache, RedisConnection};
use failsafe::{FailSafeCache, FailSafeConfiguration, FailSafeResult};
use moka::future::Cache;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, broadcast},
    task::{self, JoinHandle},
};

mod distributed_cache;
mod failsafe;

#[async_trait]
/// A trait for implementing value factories that can be used with `FusionCache`.
///
/// The factory is responsible for retrieving or generating values when they're not in the cache.
/// It must be cloneable and thread-safe since it might be called from multiple tasks.
///
/// # Type Parameters
/// * `TKey` - The type of keys used in the cache
/// * `TValue` - The type of values stored in the cache
/// * `TError` - The type of errors that can occur during value generation
pub trait Factory<
    TKey: Clone + Send + Sync + 'static,
    TValue: Clone + Send + Sync + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
>: Send + Sync + Clone + 'static
{
    /// Retrieves or generates a value for the given key.
    ///
    /// This method is called when a value is not found in the cache.
    /// It should implement the logic to fetch or generate the value.
    /// # Arguments
    /// * `key` - The key for which to retrieve or generate the value
    async fn get(&self, key: &TKey) -> Result<TValue, TError>;
}

pub struct FusionCacheBuilder<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
> {
    capacity: u64,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    hard_timeout: Option<std::time::Duration>,
    redis_address: Option<String>,
    application_name: Option<String>,
    should_factory_execute_in_background: bool,
    should_redis_writes_happen_in_background: bool,
    phantom_t_key: PhantomData<TKey>,
    phantom_t_value: PhantomData<TValue>,
    time_to_live: Option<std::time::Duration>,
    time_to_idle: Option<std::time::Duration>,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
> FusionCacheBuilder<TKey, TValue>
{
    pub fn new() -> Self {
        Self {
            capacity: 1000,
            fail_safe_configuration: None,
            should_factory_execute_in_background: false,
            phantom_t_key: PhantomData,
            phantom_t_value: PhantomData,
            hard_timeout: None,
            redis_address: None,
            application_name: None,
            should_redis_writes_happen_in_background: false,
            time_to_live: None,
            time_to_idle: None,
        }
    }

    /// Sets the maximum capacity of the cache.
    /// This is the maximum number of entries the cache can hold before it starts evicting old entries.
    /// The default capacity is 1000 entries.
    /// # Arguments
    /// * `capacity` - The maximum number of entries the cache can hold
    pub fn with_capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the time-to-live for entries in the cache.
    /// # Arguments
    /// * `time_to_live` - The time-to-live for entries in the cache
    pub fn with_time_to_live(mut self, time_to_live: std::time::Duration) -> Self {
        self.time_to_live = Some(time_to_live);
        self
    }

    /// Sets the time-to-idle for entries in the cache.
    /// # Arguments
    /// * `time_to_idle` - The time-to-idle for entries in the cache
    pub fn with_time_to_idle(mut self, time_to_idle: std::time::Duration) -> Self {
        self.time_to_idle = Some(time_to_idle);
        self
    }

    /// Configures the fail-safe mechanism for the cache.
    /// This allows the cache to handle situations where the factory is slow or fails to respond.
    /// # Arguments
    /// * `entry_ttl` - The time-to-live for entries in the fail-safe cache
    /// * `failsafe_cycle_ttl` - The time-to-live for a fail-safe cycle
    /// * `max_cycles` - The maximum number of cycles the fail-safe cache can go through before giving up
    /// * `soft_timeout` - An optional soft timeout for factory operations, after which the cache will try to use the fail-safe cache
    pub fn with_fail_safe(
        mut self,
        entry_ttl: std::time::Duration,
        failsafe_cycle_ttl: std::time::Duration,
        max_cycles: Option<u64>,
        soft_timeout: Option<std::time::Duration>,
    ) -> Self {
        self.fail_safe_configuration = Some(FailSafeConfiguration::new(
            entry_ttl,
            failsafe_cycle_ttl,
            max_cycles,
            soft_timeout,
        ));
        self
    }

    /// Sets a hard timeout for factory operations.
    /// If the factory does not return a value within this duration, it will return a `FusionCacheError::FactoryTimeout`.
    ///
    /// # Arguments
    /// * `timeout` - The maximum duration to wait for the factory to return a value
    pub fn with_hard_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.hard_timeout = Some(timeout);
        self
    }

    /// Configures whether the factory should execute in the background after a soft timeout.
    /// If set to `true`, the factory will continue to run in the background even after a soft timeout.
    ///
    /// # Arguments
    /// * `should_execute_in_background` - Whether the factory should execute in the background
    pub fn set_factory_background_execution(mut self, should_execute_in_background: bool) -> Self {
        self.should_factory_execute_in_background = should_execute_in_background;
        self
    }

    /// Configures Redis as a level 2 cache.
    /// If `use_as_distributed_cache` is true, it will also be used as a distributed cache, that is, Redis will also be used as a backplane for the cache.
    ///
    /// # Arguments
    /// * `address` - The address of the Redis server.
    /// * `use_as_distributed_cache` - Whether to use Redis as a distributed cache.
    pub fn with_redis(
        mut self,
        address: String,
        application_name: String,
        should_writes_happen_in_background: bool,
    ) -> Self {
        self.redis_address = Some(address);
        self.application_name = Some(application_name);
        self.should_redis_writes_happen_in_background = should_writes_happen_in_background;
        self
    }

    pub async fn build(self) -> Result<FusionCache<TKey, TValue>, FusionCacheError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000000);
        let distributed_cache = if let Some(address) = self.redis_address {
            let redis_client = redis::Client::open(address).unwrap();
            let inner_redis_connection = redis_client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| FusionCacheError::InitializationError(e.to_string()))?;
            let redis_connection = RedisConnection::new(inner_redis_connection);
            let mut distributed_cache = DistributedCache::new(
                redis_connection,
                redis_client,
                tx,
                self.application_name.unwrap(),
            );
            distributed_cache
                .start_synchronization()
                .await
                .map_err(|e| FusionCacheError::InitializationError(e.to_string()))?;
            Some(distributed_cache)
        } else {
            None
        };
        let mut cache_builder = Cache::builder().max_capacity(self.capacity);
        if let Some(time_to_live) = self.time_to_live {
            cache_builder = cache_builder.time_to_live(time_to_live);
        }
        if let Some(time_to_idle) = self.time_to_idle {
            cache_builder = cache_builder.time_to_idle(time_to_idle);
        }
        let cache: Cache<TKey, TValue> = cache_builder.build();
        let fail_safe_cache: Option<FailSafeCache<TKey, TValue>> =
            if let Some(fail_safe_configuration) = self.fail_safe_configuration {
                Some(FailSafeCache::new(fail_safe_configuration))
            } else {
                None
            };
        let _cache = cache.clone();
        let mut _fail_safe_cache = fail_safe_cache.clone();
        let cache_synchronization_task = if let Some(_) = &distributed_cache {
            Some(task::spawn(async move {
                while let Some(key) = rx.recv().await {
                    if let Some(_) = _cache.get(&key).await {
                        _cache.invalidate(&key).await;
                    }
                    if let Some(fsc) = &mut _fail_safe_cache {
                        fsc.invalidate(&key).await;
                    }
                }
            }))
        } else {
            None
        };
        Ok(FusionCache {
            cache,
            should_factory_execute_in_background: self.should_factory_execute_in_background,
            hard_timeout: self.hard_timeout,
            fail_safe_cache: fail_safe_cache.clone(),
            in_flight_factory_requests: Arc::new(Mutex::new(HashMap::new())),
            distributed_cache,
            should_redis_writes_happen_in_background: self.should_redis_writes_happen_in_background,
            _cache_synchronization_task: cache_synchronization_task
                .map(|t| Arc::new(Mutex::new(t))),
        })
    }
}

/// Errors that can occur during cache operations.
#[derive(Debug, Clone)]
pub enum FusionCacheError {
    /// A generic error occurred
    Other,
    /// System is in an inconsistent state (e.g., in-flight request tracking corrupted)
    SystemCorruption,
    /// The factory failed to generate a value
    FactoryError,
    /// The factory operation timed out
    FactoryTimeout,
    /// An error occurred during initialization of the cache
    InitializationError(String),
    /// An error occurred during Redis operations
    RedisError(String),
}

#[derive(Clone)]
pub struct FusionCache<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
> {
    cache: Cache<TKey, TValue>,
    hard_timeout: Option<std::time::Duration>,
    should_factory_execute_in_background: bool,
    fail_safe_cache: Option<FailSafeCache<TKey, TValue>>,
    // This map holds the in-flight factory requests for each key.
    in_flight_factory_requests:
        Arc<Mutex<HashMap<TKey, broadcast::Sender<Result<TValue, FusionCacheError>>>>>,
    distributed_cache: Option<DistributedCache<TKey, TValue>>,
    _cache_synchronization_task: Option<Arc<Mutex<JoinHandle<()>>>>,
    should_redis_writes_happen_in_background: bool,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
> FusionCache<TKey, TValue>
{
    /// Retrieves a value from the cache, or generates it using the provided factory if not found.
    /// If the distributed cache is enabled, the value will be retrieved and written to the distributed cache as well.
    ///
    /// This method implements a sophisticated cache stampede protection mechanism:
    /// - If the value exists in cache, returns it immediately
    /// - If the value is being generated by another request, waits for and returns that result
    /// - If no request is in flight, generates the value using the factory
    ///
    /// # Arguments
    /// * `key` - The key to look up in the cache
    /// * `factory` - The factory to use for generating the value if not found
    ///
    /// # Returns
    /// * `Ok(TValue)` - The cached or newly generated value
    /// * `Err(FusionCacheError)` - If value generation failed or timed out
    pub async fn get_or_set<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
    ) -> Result<TValue, FusionCacheError> {
        let maybe_entry = self.cache.get(&key).await;
        match maybe_entry {
            Some(entry) => Ok(entry),
            None => {
                let maybe_value_from_redis =
                    if let Some(distributed_cache) = &mut self.distributed_cache {
                        distributed_cache.get(&key).await?
                    } else {
                        None
                    };
                if let Some(value_from_redis) = maybe_value_from_redis {
                    self.cache
                        .insert(key.clone(), value_from_redis.clone())
                        .await;
                    Ok(value_from_redis)
                } else {
                    let mut in_flight_factory_requests =
                        self.in_flight_factory_requests.lock().await;
                    let maybe_factory_sender = in_flight_factory_requests.get(&key).cloned();
                    match maybe_factory_sender {
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
                    }
                }
            }
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
                    if let Some(distributed_cache) = &mut self.distributed_cache {
                        if !self.should_redis_writes_happen_in_background {
                            let _ = distributed_cache.set(&key, &factory_value).await;
                        } else {
                            let mut _distributed_cache = distributed_cache.clone();
                            let _factory_value = factory_value.clone();
                            let _key = key.clone();
                            task::spawn(async move {
                                let _ = _distributed_cache.set(&_key, &_factory_value).await;
                            });
                        }
                    }
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

    /// Retrieves a value from the cache without invoking the factory.
    ///
    /// # Arguments
    /// * `key` - The key to look up in the cache
    ///
    /// # Returns
    /// * `Some(TValue)` - The cached value if it exists
    /// * `None` - If the value is not in the cache
    pub async fn get(&mut self, key: TKey) -> Option<TValue> {
        if let Some(distributed_cache) = &mut self.distributed_cache {
            let value = distributed_cache.get(&key).await;
            if let Ok(value) = value {
                if let Some(v) = value {
                    self.cache.insert(key, v.clone()).await;
                    return Some(v);
                }
            }
        }
        self.cache.get(&key).await
    }

    /// Directly sets a value in the cache.
    ///
    /// # Arguments
    /// * `key` - The key under which to store the value
    /// * `value` - The value to store
    pub async fn set(&mut self, key: TKey, value: TValue) {
        if let Some(distributed_cache) = &mut self.distributed_cache {
            if !self.should_redis_writes_happen_in_background {
                let _ = distributed_cache.set(&key, &value).await;
            } else {
                let mut _distributed_cache = distributed_cache.clone();
                let _value = value.clone();
                let _key = key.clone();
                task::spawn(async move {
                    let _ = _distributed_cache.set(&_key, &_value).await;
                });
            }
        }
        self.cache.insert(key, value).await;
    }

    /// Removes a value from the cache.
    ///
    /// # Arguments
    /// * `key` - The key to remove from the cache
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
        global_factory_result_sender: broadcast::Sender<Result<TValue, FusionCacheError>>,
    ) -> Result<TValue, FusionCacheError> {
        if let Some(soft_timeout) = self.fail_safe_cache.as_ref().and_then(|c| c.soft_timeout()) {
            self.factory_result_with_soft_timeout(
                &key,
                &global_factory_result_sender,
                factory,
                soft_timeout,
            )
            .await
        } else if let Some(hard_timeout) = self.hard_timeout {
            let result = select! {
                factory_result = factory.get(&key) => {
                    factory_result.map_err(|e| e.into())
                }
                _ = tokio::time::sleep(hard_timeout) => {
                    Err(FusionCacheError::FactoryTimeout)
                }
            };

            self.process_factory_result_or_timeout(&key, &global_factory_result_sender, result)
                .await
        } else {
            let result = factory.get(&key).await.map_err(|e| e.into());

            self.process_factory_result_or_timeout(&key, &global_factory_result_sender, result)
                .await
        }
    }

    async fn factory_result_with_soft_timeout<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: &TKey,
        global_factory_result_sender: &broadcast::Sender<Result<TValue, FusionCacheError>>,
        factory: F,
        soft_timeout: Duration,
    ) -> Result<TValue, FusionCacheError> {
        let (local_factory_result_sender, local_factory_result_receiver) =
            tokio::sync::oneshot::channel::<Result<TValue, FusionCacheError>>();
        let background_factory = self.start_factory_in_background(
            &key,
            &factory,
            &global_factory_result_sender,
            local_factory_result_sender,
        );
        select! {
            factory_result = local_factory_result_receiver => {
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
                        let _ = global_factory_result_sender.send(Ok(value.clone()));
                        if !self.should_factory_execute_in_background {
                            background_factory.abort();
                            self.remove_from_in_flight_factory_requests(&key).await;
                        }
                        Ok(value)
                    }
                    FailSafeResult::Miss => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        if let Some(hard_timeout) = self.hard_timeout {
                            tokio::time::sleep(hard_timeout - soft_timeout).await;
                            background_factory.abort();
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
                            background_factory.abort();
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
    }

    async fn process_factory_result_or_timeout(
        &mut self,
        key: &TKey,
        global_factory_result_sender: &broadcast::Sender<Result<TValue, FusionCacheError>>,
        result: Result<TValue, FusionCacheError>,
    ) -> Result<TValue, FusionCacheError> {
        match result {
            Ok(factory_value) => {
                // I can unwrap safely because I already know that fail_safe_cache is Some
                let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                if let Some(distributed_cache) = &mut self.distributed_cache {
                    if !self.should_redis_writes_happen_in_background {
                        let _ = distributed_cache.set(&key, &factory_value).await;
                    } else {
                        let mut _distributed_cache = distributed_cache.clone();
                        let _factory_value = factory_value.clone();
                        let _key = key.clone();
                        task::spawn(async move {
                            let _ = _distributed_cache.set(&_key, &_factory_value).await;
                        });
                    }
                }
                self.cache.insert(key.clone(), factory_value.clone()).await;
                fail_safe_cache
                    .insert(key.clone(), factory_value.clone())
                    .await;
                fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                let _ = global_factory_result_sender.send(Ok(factory_value.clone()));
                self.remove_from_in_flight_factory_requests(key).await;
                Ok(factory_value)
            }
            Err(e) => {
                let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                fail_safe_cache.start_failsafe_cycle(key.clone()).await;
                let fail_safe_result = fail_safe_cache.get(key).await;
                match fail_safe_result {
                    FailSafeResult::Hit(value) => {
                        let _ = global_factory_result_sender.send(Ok(value.clone()));
                        self.remove_from_in_flight_factory_requests(key).await;
                        Ok(value)
                    }
                    FailSafeResult::Miss => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        let _ = global_factory_result_sender.send(Err(e));
                        self.remove_from_in_flight_factory_requests(key).await;
                        Err(FusionCacheError::FactoryError)
                    }
                    FailSafeResult::TooManyCycles => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        let _ = global_factory_result_sender.send(Err(e));
                        self.remove_from_in_flight_factory_requests(key).await;
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

    fn start_factory_in_background<
        TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: &TKey,
        factory: &F,
        global_factory_result_sender: &broadcast::Sender<Result<TValue, FusionCacheError>>,
        tx: tokio::sync::oneshot::Sender<Result<TValue, FusionCacheError>>,
    ) -> task::JoinHandle<()> {
        let _key = key.clone();
        let _factory = factory.clone();
        let _factory_sender = global_factory_result_sender.clone();
        let mut _self = self.clone();
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
        factory_task
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
        let cache = FusionCacheBuilder::new().build().await.unwrap();
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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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
            .build()
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_distributed_cache_with_redis() {
        let factory = TestFactory::new();
        let mut cache: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_with_redis".to_string(),
                false,
            )
            .build()
            .await
            .unwrap();

        // Test setting and getting a value
        let key = 1;
        let value = cache.get_or_set(key, factory.clone()).await.unwrap();
        assert_eq!(value, 1);

        // Test getting the value again (should be cached)
        let value = cache.get_or_set(key, factory.clone()).await.unwrap();
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn test_distributed_cache_synchronization() {
        let factory = TestFactory::new();
        let mut cache1: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_synchronization".to_string(),
                false,
            )
            .build()
            .await
            .unwrap();

        let mut cache2: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_synchronization".to_string(),
                false,
            )
            .build()
            .await
            .unwrap();

        // Set value in cache1
        let key = 1;
        let value = cache1.get_or_set(key, factory.clone()).await.unwrap();
        assert_eq!(value, 1);

        // Wait for synchronization
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Value should be in cache2
        let value = cache2.get(key).await.unwrap();
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn test_distributed_cache_with_redis_connection_failure() {
        let factory = TestFactory::new();
        let mut cache: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_with_redis_connection_failure".to_string(),
                false,
            )
            .build()
            .await
            .unwrap();

        // Set initial value
        let key = 1;
        let value = cache.get_or_set(key, factory.clone()).await.unwrap();
        assert_eq!(value, 1);

        // Break the Redis connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.break_connection();
        }

        cache.set(key, 2).await;

        // Restore the connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.restore_connection();
        }

        // Wait for auto-recovery
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Value should be evicted
        let value = cache.get(key).await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_distributed_cache_with_redis_and_failsafe() {
        let factory = FallibleTestFactory::new();
        let mut cache: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_with_redis_and_failsafe".to_string(),
                false,
            )
            .with_fail_safe(
                Duration::from_secs(10),
                Duration::from_secs(5),
                Some(3),
                None,
            )
            .build()
            .await
            .unwrap();

        // Set initial value
        let key = 1;
        let value = cache.get_or_set(key, factory.clone()).await.unwrap();
        assert_eq!(value, 1);

        // Break the Redis connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.break_connection();
        }

        // Try to get the value (should use failsafe)
        cache.set(key, 2).await;

        // Restore the connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.restore_connection();
        }

        // Wait for auto-recovery
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Value should be evicted
        let value = cache.get(key).await;
        assert_eq!(value, None);
    }
}
