use async_trait::async_trait;
use distributed_cache::{DistributedCache, RedisConnection};
use failsafe::{FailSafeCache, FailSafeConfiguration, FailSafeResult};
use moka::{Expiry, future::Cache};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};
use tokio::{
    select,
    sync::{Mutex, broadcast},
    task::{self, JoinHandle},
};
use tracing::{debug, error, info, warn};

mod distributed_cache;
mod failsafe;

const LOG_TARGET: &str = "fusioncache";

pub(crate) enum Source<TValue: Clone + Send + Sync + 'static> {
    Factory(TValue),
    Failsafe(TValue),
}

impl<TValue: Clone + Send + Sync + 'static> Source<TValue> {
    pub(crate) fn value(&self) -> TValue {
        match self {
            Self::Factory(value) => value.clone(),
            Self::Failsafe(value) => value.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheValue<TValue: Clone + Send + Sync + 'static> {
    value: TValue,
    time_to_live: Option<std::time::Duration>,
    time_to_idle: Option<std::time::Duration>,
}

impl<TValue: Clone + Send + Sync + 'static> CacheValue<TValue> {
    pub fn new(
        value: TValue,
        time_to_live: Option<std::time::Duration>,
        time_to_idle: Option<std::time::Duration>,
    ) -> Self {
        Self {
            value,
            time_to_live,
            time_to_idle,
        }
    }
}

pub(crate) struct CacheValueExpiry;

impl<TKey: Clone + Send + Sync + 'static, TValue: Clone + Send + Sync + 'static>
    Expiry<TKey, CacheValue<TValue>> for CacheValueExpiry
{
    fn expire_after_create(
        &self,
        _key: &TKey,
        value: &CacheValue<TValue>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        value.time_to_live
    }

    fn expire_after_read(
        &self,
        _key: &TKey,
        value: &CacheValue<TValue>,
        read_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
        last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        value.time_to_idle.map(|time_to_idle| {
            let time_since_last_modified = read_at.duration_since(last_modified_at);
            time_to_idle - time_since_last_modified
        })
    }

    fn expire_after_update(
        &self,
        _key: &TKey,
        value: &CacheValue<TValue>,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        value.time_to_live
    }
}

#[derive(Clone, Debug)]
pub struct FusionCacheOptions {
    entry_ttl: Option<std::time::Duration>,
    ignore_ttl: bool,
    entry_tti: Option<std::time::Duration>,
    ignore_tti: bool,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    skip_fail_safe_cache: bool,
    hard_timeout: Option<std::time::Duration>,
    skip_hard_timeout: bool,
    skip_distributed_cache: bool,
    should_redis_writes_happen_in_background: Option<bool>,
}

pub struct FusionCacheOptionsBuilder {
    entry_ttl: Option<std::time::Duration>,
    ignore_ttl: bool,
    entry_tti: Option<std::time::Duration>,
    ignore_tti: bool,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    skip_fail_safe_cache: bool,
    hard_timeout: Option<std::time::Duration>,
    skip_hard_timeout: bool,
    skip_distributed_cache: bool,
    should_redis_writes_happen_in_background: Option<bool>,
}

impl FusionCacheOptionsBuilder {
    pub fn new() -> Self {
        Self {
            entry_ttl: None,
            ignore_ttl: false,
            entry_tti: None,
            ignore_tti: false,
            fail_safe_configuration: None,
            skip_fail_safe_cache: false,
            hard_timeout: None,
            skip_hard_timeout: false,
            skip_distributed_cache: false,
            should_redis_writes_happen_in_background: None,
        }
    }

    /// Sets the time-to-live for the cache.
    ///
    /// # Arguments
    ///
    /// * `time_to_live` - The time-to-live for the cache.
    pub fn with_time_to_live(mut self, time_to_live: std::time::Duration) -> Self {
        self.entry_ttl = Some(time_to_live);
        self
    }

    /// Ignores the time-to-live for the cache.
    pub fn ignore_time_to_live(mut self) -> Self {
        self.ignore_ttl = true;
        self
    }

    /// Sets the time-to-idle for the cache.
    ///
    /// # Arguments
    ///
    /// * `time_to_idle` - The time-to-idle for the cache.
    pub fn with_time_to_idle(mut self, time_to_idle: std::time::Duration) -> Self {
        self.entry_tti = Some(time_to_idle);
        self
    }

    /// Ignores the time-to-idle for the cache.
    pub fn ignore_time_to_idle(mut self) -> Self {
        self.ignore_tti = true;
        self
    }

    /// Sets the fail-safe configuration for the cache.
    ///
    /// # Arguments
    ///
    /// * `entry_ttl` - The time-to-live for the fail-safe cache.
    /// * `failsafe_cycle_ttl` - The time-to-live for the fail-safe cycle.
    /// * `max_cycles` - The maximum number of fail-safe cycles.
    /// * `soft_timeout` - The soft timeout for the fail-safe cache.
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

    /// Skips the fail-safe cache for the cache.
    pub fn skip_fail_safe_cache(mut self) -> Self {
        self.skip_fail_safe_cache = true;
        self
    }

    /// Sets the hard timeout for the cache.
    ///
    /// # Arguments
    ///
    /// * `hard_timeout` - The hard timeout for the cache.
    pub fn with_hard_timeout(mut self, hard_timeout: std::time::Duration) -> Self {
        self.hard_timeout = Some(hard_timeout);
        self
    }

    /// Skips the hard timeout for the cache.
    pub fn skip_hard_timeout(mut self) -> Self {
        self.skip_hard_timeout = true;
        self
    }

    /// Skips the distributed cache for the cache.
    pub fn skip_distributed_cache(mut self) -> Self {
        self.skip_distributed_cache = true;
        self
    }

    pub fn build(self) -> FusionCacheOptions {
        FusionCacheOptions {
            entry_ttl: self.entry_ttl,
            ignore_ttl: self.ignore_ttl,
            entry_tti: self.entry_tti,
            ignore_tti: self.ignore_tti,
            fail_safe_configuration: self.fail_safe_configuration,
            skip_fail_safe_cache: self.skip_fail_safe_cache,
            hard_timeout: self.hard_timeout,
            skip_hard_timeout: self.skip_hard_timeout,
            skip_distributed_cache: self.skip_distributed_cache,
            should_redis_writes_happen_in_background: self.should_redis_writes_happen_in_background,
        }
    }
}

impl FusionCacheOptions {
    fn with_overrides(self, overrides: Option<FusionCacheOptions>) -> FusionCacheOptions {
        if let Some(overrides) = overrides {
            FusionCacheOptions {
                fail_safe_configuration: if let Some(fsc_override) =
                    overrides.fail_safe_configuration
                {
                    Some(fsc_override)
                } else if !overrides.skip_fail_safe_cache {
                    self.fail_safe_configuration
                } else {
                    None
                },
                hard_timeout: if let Some(ht_override) = overrides.hard_timeout {
                    Some(ht_override)
                } else if !overrides.skip_hard_timeout {
                    self.hard_timeout
                } else {
                    None
                },
                entry_ttl: if let Some(ttl_override) = overrides.entry_ttl {
                    Some(ttl_override)
                } else if !overrides.ignore_ttl {
                    self.entry_ttl
                } else {
                    None
                },
                entry_tti: if let Some(tti_override) = overrides.entry_tti {
                    Some(tti_override)
                } else if !overrides.ignore_tti {
                    self.entry_tti
                } else {
                    None
                },
                skip_distributed_cache: overrides.skip_distributed_cache,
                ignore_ttl: overrides.ignore_ttl,
                ignore_tti: overrides.ignore_tti,
                should_redis_writes_happen_in_background: if let Some(
                    should_redis_writes_happen_in_background,
                ) =
                    overrides.should_redis_writes_happen_in_background
                {
                    Some(should_redis_writes_happen_in_background)
                } else {
                    self.should_redis_writes_happen_in_background
                },
                skip_fail_safe_cache: overrides.skip_fail_safe_cache,
                skip_hard_timeout: overrides.skip_hard_timeout,
            }
        } else {
            FusionCacheOptions { ..self }
        }
    }
}

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
    TError: Clone + Send + Sync + 'static,
>: Send + Sync + Clone + Debug + 'static
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
    entry_ttl: Option<std::time::Duration>,
    entry_tti: Option<std::time::Duration>,
    distributed_cache_entry_ttl: Option<Duration>,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + Debug + 'static,
    TValue: Clone + Send + Sync + Serialize + DeserializeOwned + Debug + 'static,
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
            entry_ttl: None,
            entry_tti: None,
            distributed_cache_entry_ttl: None,
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
        self.entry_ttl = Some(time_to_live);
        self
    }

    /// Sets the time-to-idle for entries in the cache.
    /// # Arguments
    /// * `time_to_idle` - The time-to-idle for entries in the cache
    pub fn with_time_to_idle(mut self, time_to_idle: std::time::Duration) -> Self {
        self.entry_tti = Some(time_to_idle);
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
    /// * `application_name` - The name of the application.
    /// * `should_writes_happen_in_background` - Whether the writes to Redis should happen in the background.
    /// * `entry_ttl` - The time-to-live for entries in the Redis cache.
    pub fn with_redis(
        mut self,
        address: String,
        application_name: String,
        should_writes_happen_in_background: bool,
        entry_ttl: Option<std::time::Duration>,
    ) -> Self {
        self.redis_address = Some(address);
        self.application_name = Some(application_name);
        self.should_redis_writes_happen_in_background = should_writes_happen_in_background;
        self.distributed_cache_entry_ttl = entry_ttl;
        self
    }

    pub async fn build(self) -> Result<FusionCache<TKey, TValue>, FusionCacheError> {
        let (distributed_cache_eviction_sender, mut distributed_cache_eviction_receiver) =
            tokio::sync::mpsc::channel(1000000);
        let distributed_cache = if let Some(address) = self.redis_address {
            debug!(target: LOG_TARGET, "Building distributed cache with Redis address: {}", address);
            let redis_client = redis::Client::open(address).unwrap();
            let inner_redis_connection = redis_client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| FusionCacheError::InitializationError(e.to_string()))?;
            let redis_connection = RedisConnection::new(inner_redis_connection);
            let mut distributed_cache = DistributedCache::new(
                redis_connection,
                redis_client,
                distributed_cache_eviction_sender,
                self.application_name.unwrap(),
                self.distributed_cache_entry_ttl,
            );
            distributed_cache
                .start_synchronization()
                .await
                .map_err(|e| FusionCacheError::InitializationError(e.to_string()))?;
            Some(distributed_cache)
        } else {
            debug!(target: LOG_TARGET, "No distributed cache configured");
            None
        };
        let cache_builder = Cache::builder()
            .expire_after(CacheValueExpiry)
            .max_capacity(self.capacity);
        let cache: Cache<TKey, CacheValue<TValue>> = cache_builder.build();
        let fail_safe_cache: Option<FailSafeCache<TKey, CacheValue<TValue>>> =
            if let Some(fail_safe_configuration) = self.fail_safe_configuration {
                debug!(
                    target: LOG_TARGET,
                    "Setting fail-safe cache with configuration: {:?}",
                    fail_safe_configuration
                );
                Some(FailSafeCache::new(fail_safe_configuration))
            } else {
                debug!(target: LOG_TARGET, "No fail-safe cache configuration set for cache");
                None
            };
        let _cache = cache.clone();
        let mut _fail_safe_cache = fail_safe_cache.clone();
        let cache_synchronization_task = if let Some(_) = &distributed_cache {
            Some(task::spawn(async move {
                while let Some(key) = distributed_cache_eviction_receiver.recv().await {
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
            time_to_live: self.entry_ttl,
            time_to_idle: self.entry_tti,
            should_factory_execute_in_background: self.should_factory_execute_in_background,
            hard_timeout: self.hard_timeout,
            fail_safe_cache: fail_safe_cache.clone(),
            in_flight_factory_requests: Arc::new(Mutex::new(HashMap::new())),
            distributed_cache,
            should_redis_writes_happen_in_background: self.should_redis_writes_happen_in_background,
            _cache_synchronization_task: cache_synchronization_task
                .map(|t| Arc::new(Mutex::new(t))),
            background_factory_tasks: Arc::new(Mutex::new(HashMap::new())),
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
    FactoryError(String),
    /// The factory operation timed out
    FactoryTimeout,
    /// An error occurred during initialization of the cache
    InitializationError(String),
    /// An error occurred during Redis operations
    RedisError(String),
}

impl Display for FusionCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FusionCacheError::Other => write!(f, "Other"),
            FusionCacheError::SystemCorruption => write!(f, "SystemCorruption"),
            FusionCacheError::FactoryError(error_message) => write!(f, "{}", error_message),
            FusionCacheError::FactoryTimeout => write!(f, "FactoryTimeout"),
            FusionCacheError::InitializationError(error_message) => {
                write!(f, "{}", error_message)
            }
            FusionCacheError::RedisError(error_message) => {
                write!(f, "{}", error_message)
            }
        }
    }
}

/// The main struct of the fusioncache-rs library.
///
/// This struct is used to create a cache that can be used to store and retrieve values.
#[derive(Clone, Debug)]
pub struct FusionCache<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
> {
    cache: Cache<TKey, CacheValue<TValue>>,
    time_to_live: Option<std::time::Duration>,
    time_to_idle: Option<std::time::Duration>,
    hard_timeout: Option<std::time::Duration>,
    should_factory_execute_in_background: bool,
    fail_safe_cache: Option<FailSafeCache<TKey, CacheValue<TValue>>>,
    // This map holds the in-flight factory requests for each key.
    in_flight_factory_requests:
        Arc<Mutex<HashMap<TKey, broadcast::Sender<Result<TValue, FusionCacheError>>>>>,
    distributed_cache: Option<DistributedCache<TKey, TValue>>,
    _cache_synchronization_task: Option<Arc<Mutex<JoinHandle<()>>>>,
    should_redis_writes_happen_in_background: bool,
    background_factory_tasks: Arc<
        Mutex<
            HashMap<
                TKey,
                (
                    JoinHandle<()>,
                    broadcast::Sender<Result<TValue, FusionCacheError>>,
                ),
            >,
        >,
    >,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + Serialize + DeserializeOwned + Debug + 'static,
    TValue: Clone + Send + Sync + Clone + Serialize + DeserializeOwned + Debug + 'static,
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
    /// * `options` - The options to use for this specifc entry. If not provided, the default options will be used.
    /// # Returns
    /// * `Ok(TValue)` - The cached or newly generated value
    /// * `Err(FusionCacheError)` - If value generation failed or timed out
    #[tracing::instrument(name = "FusionCache::get_or_set", skip(self))]
    pub async fn get_or_set<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        options: Option<FusionCacheOptions>,
    ) -> Result<TValue, FusionCacheError> {
        let options = self.get_options().with_overrides(options);
        info!(target: LOG_TARGET, "Cache lookup for key: {:?}", key);
        let maybe_entry = self.cache.get(&key).await;
        match maybe_entry {
            Some(entry) => {
                info!(target: LOG_TARGET, "Cache hit for key: {:?}", key);
                Ok(entry.value)
            }
            None => {
                let maybe_value_from_redis =
                    if let Some(distributed_cache) = &mut self.distributed_cache {
                        distributed_cache.get(&key).await?
                    } else {
                        None
                    };
                if let Some(value_from_redis) = maybe_value_from_redis {
                    info!(target: LOG_TARGET, "Cache miss for key: {:?}, hit in Redis", key);
                    self.cache
                        .insert(key.clone(), value_from_redis.clone())
                        .await;
                    Ok(value_from_redis.value)
                } else {
                    info!(target: LOG_TARGET, "Cache miss for key: {:?}. Retrieving from factory", key);
                    self.request_key(key, factory, options).await
                }
            }
        }
    }

    #[tracing::instrument(name = "FusionCache::request_key", skip(self))]
    async fn request_key<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        options: FusionCacheOptions,
    ) -> Result<TValue, FusionCacheError> {
        let mut in_flight_factory_requests = self.in_flight_factory_requests.lock().await;
        let maybe_factory_sender = in_flight_factory_requests.get(&key).cloned();
        match maybe_factory_sender {
            Some(factory_sender) => {
                info!(target: LOG_TARGET, "Factory already in flight for key: {:?}, waiting", key);
                drop(in_flight_factory_requests);
                let mut factory_receiver = factory_sender.subscribe();
                let factory_receiver_result = factory_receiver.recv().await;
                if let Ok(factory_result) = factory_receiver_result {
                    info!(target: LOG_TARGET, "Factory result received for key: {:?}", key);
                    factory_result
                } else {
                    // The value might be in the cache, but the receiver could return an error because
                    // the sender was dropped. In this case, we should try to get the value from the cache.
                    if let Some(value) = self.cache.get(&key).await.map(|entry| entry.value) {
                        info!(target: LOG_TARGET, "Factory result received for key: {:?}, but receiver was dropped. Value was in cache. Returning value", key);
                        Ok(value)
                    } else {
                        error!(target: LOG_TARGET, "There's a very high probablity that your system is corrupted. Factory result received for key: {:?}, but receiver was dropped, and the value was not in cache. Returning error.", key);
                        Err(FusionCacheError::SystemCorruption)
                    }
                }
            }
            None => {
                info!(target: LOG_TARGET, "No factory in flight for key: {:?}, creating new factory", key);
                let (factory_sender, _) = broadcast::channel(1);
                in_flight_factory_requests.insert(key.clone(), factory_sender.clone());
                drop(in_flight_factory_requests);
                let factory_value_or_failsafe = self
                    .get_from_factory_or_fail_safe(key.clone(), factory, options.clone())
                    .await;

                match &factory_value_or_failsafe {
                    Ok(Source::Factory(value)) => {
                        info!(target: LOG_TARGET, "Factory result received for key: {:?}, setting value in cache", key);
                        self.set_internal(key.clone(), value.clone(), options).await;
                    }
                    Ok(Source::Failsafe(_)) => {
                        info!(target: LOG_TARGET, "The factory errored out, however failsafe result received for key: {:?}, setting value in cache. Returning failsafe value.", key);
                    }
                    Err(e) => {
                        error!(target: LOG_TARGET, "Error received from factory: {:?}. Returning error.", e);
                    }
                }

                let retrieval_result = factory_value_or_failsafe.map(|s| s.value());
                let _ = factory_sender.send(retrieval_result.clone());
                self.remove_from_in_flight_factory_requests(&key).await;
                retrieval_result
            }
        }
    }

    #[tracing::instrument(name = "FusionCache::get_from_factory_or_fail_safe", skip(self))]
    async fn get_from_factory_or_fail_safe<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        options: FusionCacheOptions,
    ) -> Result<Source<TValue>, FusionCacheError> {
        if let Some(fail_safe_cache) = &mut self.fail_safe_cache {
            let fail_safe_result = fail_safe_cache.get(&key).await;
            match fail_safe_result {
                FailSafeResult::NotInFailSafeMode | FailSafeResult::CurrentCycleEnded => {
                    info!(target: LOG_TARGET, "Not in fail-safe mode for key: {:?}, getting value from factory", key);
                    self.do_get_from_factory_or_failsafe(key.clone(), factory, options)
                        .await
                }
                FailSafeResult::Hit(value) => {
                    warn!(target: LOG_TARGET, "Currently in fail-safe mode. Fail-safe cache hit for key: {:?}, returning failsafe value", key);
                    Ok(Source::Failsafe(value.value))
                }
                FailSafeResult::Miss(error_message) => {
                    error!(target: LOG_TARGET, "Fail-safe cache miss for key: {:?}, exiting fail-safe mode", key);
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    Err(FusionCacheError::FactoryError(error_message))
                }
                FailSafeResult::TooManyCycles(error_message) => {
                    error!(target: LOG_TARGET, "Fail-safe cache too many cycles for key: {:?}, exiting fail-safe mode", key);
                    fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                    fail_safe_cache.invalidate(&key).await;
                    Err(FusionCacheError::FactoryError(error_message))
                }
            }
        } else {
            if let Some(hard_timeout) = self.hard_timeout {
                let result_or_timeout = tokio::time::timeout(hard_timeout, factory.get(&key)).await;
                match result_or_timeout {
                    Ok(factory_value) => factory_value
                        .map_err(|e| FusionCacheError::FactoryError(e.to_string()))
                        .map(Source::Factory),
                    Err(_) => {
                        error!(target: LOG_TARGET, "Factory operation timed out for key: {:?} on hard timeout, returning error", key);
                        Err(FusionCacheError::FactoryTimeout)
                    }
                }
            } else {
                info!(target: LOG_TARGET, "No timeouts configured, getting value from factory for key: {:?}", key);
                factory
                    .get(&key)
                    .await
                    .map_err(|e| FusionCacheError::FactoryError(e.to_string()))
                    .map(Source::Factory)
            }
        }
    }

    async fn remove_from_in_flight_factory_requests(&self, key: &TKey) {
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
    #[tracing::instrument(name = "FusionCache::get", skip(self))]
    pub async fn get(&mut self, key: TKey) -> Option<TValue> {
        if let Some(distributed_cache) = &mut self.distributed_cache {
            let value = distributed_cache.get(&key).await;
            if let Ok(value) = value {
                if let Some(v) = value {
                    self.cache.insert(key, v.clone()).await;
                    return Some(v.value);
                }
            }
        }
        self.cache.get(&key).await.map(|entry| entry.value)
    }

    /// Directly sets a value in the cache.
    ///
    /// # Arguments
    /// * `key` - The key under which to store the value
    /// * `value` - The value to store
    /// * `options` - The options to use for this specifc entry. If not provided, the default options will be used.
    #[tracing::instrument(name = "FusionCache::set", skip(self))]
    pub async fn set(&mut self, key: TKey, value: TValue, options: Option<FusionCacheOptions>) {
        let opts = options.unwrap_or(self.get_options());
        self.set_internal(key, value, opts).await;
    }

    #[tracing::instrument(name = "FusionCache::set_internal", skip(self))]
    async fn set_internal(&mut self, key: TKey, value: TValue, options: FusionCacheOptions) {
        let cache_value = CacheValue::new(value, options.entry_ttl, options.entry_tti);
        if let Some(distributed_cache) = &mut self.distributed_cache {
            if !options.skip_distributed_cache {
                if !options
                    .should_redis_writes_happen_in_background
                    .is_some_and(|bw| bw)
                {
                    info!(target: LOG_TARGET, "Setting value in distributed cache for key: {:?}", key);
                    let _ = distributed_cache.set(&key, &cache_value).await;
                } else {
                    let mut _distributed_cache = distributed_cache.clone();
                    let _cache_value = cache_value.clone();
                    let _key = key.clone();
                    info!(target: LOG_TARGET, "Spawning task to set value in distributed cache for key: {:?}", key);
                    task::spawn(async move {
                        let _ = _distributed_cache.set(&_key, &_cache_value).await;
                    });
                }
            } else {
                info!(target: LOG_TARGET, "Skipping distributed cache write for key: {:?}", key);
            }
        } else {
            info!(target: LOG_TARGET, "No distributed cache configured, skipping distributed cache write for key: {:?}", key);
        }
        if let Some(fail_safe_cache) = &mut self.fail_safe_cache {
            info!(target: LOG_TARGET, "Setting value in fail-safe cache for key: {:?}", key);
            fail_safe_cache
                .insert(key.clone(), cache_value.clone())
                .await;
            fail_safe_cache.exit_failsafe_mode(key.clone()).await;
        }
        info!(target: LOG_TARGET, "Setting value in local cache for key: {:?}", key);
        self.cache.insert(key, cache_value).await;
    }

    /// Removes a value from the cache.
    ///
    /// # Arguments
    /// * `key` - The key to remove from the cache
    pub async fn evict(&mut self, key: TKey) {
        self.cache.invalidate(&key).await;
        if let Some(distributed_cache) = &mut self.distributed_cache {
            distributed_cache.evict(&key).await;
        }
    }
    // This method only gets called when the cache is created with fail-safe enabled.
    #[tracing::instrument(name = "FusionCache::do_get_from_factory_or_failsafe", skip(self))]
    async fn do_get_from_factory_or_failsafe<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: TKey,
        factory: F,
        options: FusionCacheOptions,
    ) -> Result<Source<TValue>, FusionCacheError> {
        if let Some(_) = options
            .fail_safe_configuration
            .as_ref()
            .and_then(|c| c.soft_timeout)
        {
            info!(target: LOG_TARGET, "Getting value from factory or failsafe cache for key: {:?}", key);
            self.factory_result_with_soft_timeout(&key, factory, options)
                .await
        } else if let Some(hard_timeout) = self.hard_timeout {
            let result = select! {
                factory_result = factory.get(&key) => {
                    factory_result.map_err(|e| FusionCacheError::FactoryError(e.to_string()))
                }
                _ = tokio::time::sleep(hard_timeout) => {
                    Err(FusionCacheError::FactoryTimeout)
                }
            };

            self.process_factory_result_or_timeout(&key, result).await
        } else {
            let result = factory
                .get(&key)
                .await
                .map_err(|e| FusionCacheError::FactoryError(e.to_string()));

            self.process_factory_result_or_timeout(&key, result).await
        }
    }

    #[tracing::instrument(name = "FusionCache::factory_result_with_soft_timeout", skip(self))]
    async fn factory_result_with_soft_timeout<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: &TKey,
        factory: F,
        options: FusionCacheOptions,
    ) -> Result<Source<TValue>, FusionCacheError> {
        let soft_timeout = options
            .fail_safe_configuration
            .as_ref()
            .unwrap()
            .soft_timeout
            .unwrap();
        let local_factory_result_sender = self
            .get_or_create_background_factory_task(key, factory, options)
            .await;
        let mut local_factory_result_receiver = local_factory_result_sender.subscribe();
        select! {
            factory_result = local_factory_result_receiver.recv() => {
                if let Ok(r) = factory_result {
                    if let Ok(value) = r {
                        Ok(Source::Factory(value))
                    } else {
                        let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                        fail_safe_cache.start_failsafe_cycle(key.clone(), r.unwrap_err().to_string()).await;
                        let fail_safe_result = fail_safe_cache.get(&key).await;
                        match fail_safe_result {
                            FailSafeResult::Hit(value)=> {
                                Ok(Source::Failsafe(value.value))
                            }
                            FailSafeResult::Miss(error_message)=>{
                                fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                                Err(FusionCacheError::FactoryError(error_message))
                            }
                            FailSafeResult::TooManyCycles(error_message)=>{
                                fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                                fail_safe_cache.invalidate(&key).await;
                                Err(FusionCacheError::FactoryError(error_message))
                            }
                            FailSafeResult::CurrentCycleEnded=>{
                                panic!("Got FailSafeResult::CurrentCycleEnded, but we just started a cycle");
                            }
                            FailSafeResult::NotInFailSafeMode => {
                                panic!("Got FailSafeResult::NotInFailSafeMode, but we just entered fail safe mode");
                            }
                                                    }
                    }
                } else {
                    Err(FusionCacheError::SystemCorruption)
                }
            }
            _ = tokio::time::sleep(soft_timeout) => {
                let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                fail_safe_cache.start_failsafe_cycle(key.clone(), "timeout".to_string()).await;
                let fail_safe_result = fail_safe_cache.get(&key).await;
                match fail_safe_result {
                    FailSafeResult::Hit(value) => {
                        if !self.should_factory_execute_in_background {
                            let mut background_factory_tasks = self.background_factory_tasks.lock().await;
                            let task = background_factory_tasks.remove(&key);
                            drop(background_factory_tasks);
                            if let Some((task, _)) = task {
                                task.abort();
                            }
                        }
                        Ok(Source::Failsafe(value.value))
                    }
                    FailSafeResult::Miss(_) => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        if let Some(hard_timeout) = self.hard_timeout {
                            tokio::time::sleep(hard_timeout - soft_timeout).await;
                            let mut background_factory_tasks = self.background_factory_tasks.lock().await;
                            let task = background_factory_tasks.remove(&key);
                            drop(background_factory_tasks);
                            if let Some((task, _)) = task {
                                task.abort();
                            }
                            Err(FusionCacheError::FactoryTimeout)
                        } else {
                            Err(FusionCacheError::FactoryTimeout)
                        }
                    }
                    FailSafeResult::TooManyCycles(error_message) => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        if let Some(hard_timeout) = self.hard_timeout {
                            tokio::time::sleep(hard_timeout - soft_timeout).await;
                            let mut background_factory_tasks = self.background_factory_tasks.lock().await;
                            let task = background_factory_tasks.remove(&key);
                            drop(background_factory_tasks);
                            if let Some((task, _)) = task {
                                task.abort();
                            }
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

    // If the factory is already in execution, we use the existing sender and task.
    // Otherwise, we create a new sender and task.
    // This is because we want to avoid creating a new task for the same key if it's already in execution.
    async fn get_or_create_background_factory_task<
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &mut self,
        key: &TKey,
        factory: F,
        options: FusionCacheOptions,
    ) -> broadcast::Sender<Result<TValue, FusionCacheError>> {
        let mut background_factory_tasks = self.background_factory_tasks.lock().await;
        let background_factory_in_execution = background_factory_tasks.remove(&key);

        let (factory_task, local_factory_result_sender) =
            if let Some((f, local_factory_result_sender)) = background_factory_in_execution {
                (f, local_factory_result_sender.clone())
            } else {
                let (local_factory_result_sender, _) =
                    tokio::sync::broadcast::channel::<Result<TValue, FusionCacheError>>(1);
                (
                    self.start_factory_in_background(
                        &key,
                        &factory,
                        local_factory_result_sender.clone(),
                        options,
                    ),
                    local_factory_result_sender.clone(),
                )
            };
        background_factory_tasks.insert(
            key.clone(),
            (factory_task, local_factory_result_sender.clone()),
        );
        drop(background_factory_tasks);
        local_factory_result_sender
    }

    async fn process_factory_result_or_timeout(
        &mut self,
        key: &TKey,
        result: Result<TValue, FusionCacheError>,
    ) -> Result<Source<TValue>, FusionCacheError> {
        match result {
            Ok(factory_value) => Ok(Source::Factory(factory_value)),
            Err(e) => {
                let fail_safe_cache = self.fail_safe_cache.as_mut().unwrap();
                fail_safe_cache
                    .start_failsafe_cycle(key.clone(), e.to_string())
                    .await;
                let fail_safe_result = fail_safe_cache.get(key).await;
                match fail_safe_result {
                    FailSafeResult::Hit(value) => Ok(Source::Failsafe(value.value)),
                    FailSafeResult::Miss(error_message) => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        Err(FusionCacheError::FactoryError(error_message))
                    }
                    FailSafeResult::TooManyCycles(error_message) => {
                        fail_safe_cache.exit_failsafe_mode(key.clone()).await;
                        Err(FusionCacheError::FactoryError(error_message))
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
        TError: Clone + Send + Sync + Display + 'static,
        F: Factory<TKey, TValue, TError>,
    >(
        &self,
        key: &TKey,
        factory: &F,
        tx: tokio::sync::broadcast::Sender<Result<TValue, FusionCacheError>>,
        options: FusionCacheOptions,
    ) -> task::JoinHandle<()> {
        let _key = key.clone();
        let _factory = factory.clone();
        let mut _self = self.clone();
        task::spawn(async move {
            let hard_timeout = _self.hard_timeout.unwrap_or(Duration::MAX);
            let factory_result = select! {
                result = _factory.get(&_key) => {
                    result.map_err(|e| FusionCacheError::FactoryError(e.to_string()))
                },
                _ = tokio::time::sleep(hard_timeout) => {
                    Err(FusionCacheError::FactoryTimeout)
                }
            };

            match factory_result {
                Ok(factory_value) => {
                    let _ = tx.send(Ok(factory_value.clone()));
                    _self
                        .set_internal(_key.clone(), factory_value, options)
                        .await;
                }
                Err(e) => {
                    _self.remove_from_in_flight_factory_requests(&_key).await;
                    let _ = tx.send(Err(e.into()));
                }
            };
            let mut background_factory_tasks = _self.background_factory_tasks.lock().await;
            background_factory_tasks.remove(&_key);
            drop(background_factory_tasks);
        })
    }

    fn get_options(&self) -> FusionCacheOptions {
        FusionCacheOptions {
            entry_ttl: self.time_to_live,
            entry_tti: self.time_to_idle,
            fail_safe_configuration: self
                .fail_safe_cache
                .as_ref()
                .map(|c| c.configuration.clone()),
            hard_timeout: self.hard_timeout.clone(),
            skip_distributed_cache: self.distributed_cache.is_none(),
            ignore_ttl: self.time_to_live.is_none(),
            ignore_tti: self.time_to_idle.is_none(),
            skip_fail_safe_cache: self.fail_safe_cache.is_none(),
            skip_hard_timeout: self.hard_timeout.is_none(),
            should_redis_writes_happen_in_background: Some(
                self.should_redis_writes_happen_in_background,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::fmt;
    use std::time::Duration;

    use super::*;

    use futures::future::join_all;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::{
        fmt::{FmtContext, FormatEvent, format},
        registry::LookupSpan,
    };

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
                let value = cache.get_or_set(key, factory.clone(), None).await.unwrap();
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let failsafe_value = cache.get_or_set(1, factory.clone(), None).await;
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone(), None).await.unwrap();
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone(), None).await;
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone(), None).await;
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let failsafe_value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, failsafe_value.unwrap());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, failsafe_value.unwrap());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let failsafe_value = cache.get_or_set(1, factory.clone(), None).await;
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

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        cache.evict(1).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone(), None).await;
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
                let value = cache.get_or_set(1, factory.clone(), None).await;
                assert_eq!(1, value.unwrap());
            }));
        }
        join_all(handles).await;

        let mut handles = vec![];
        for _ in 0..100000 {
            let factory = factory.clone();
            let mut cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let value = cache.get_or_set(1, factory.clone(), None).await;
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
        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        // Second call should hit failsafe before factory completes
        let start = std::time::Instant::now();
        let value = cache.get_or_set(1, factory.clone(), None).await;
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

        let result = cache.get_or_set(1, factory, None).await;
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
        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        // Trigger failsafe hit and background execution
        cache.evict(1).await;
        let value = cache.get_or_set(1, factory.clone(), None).await;
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
        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        // Trigger failsafe hit and background execution
        cache.evict(1).await;
        let value = cache.get_or_set(1, factory.clone(), None).await;
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

    #[tokio::test]
    async fn test_entry_is_evicted_after_cache_ttl_expires() {
        let factory = TestFactory::new();
        let mut cache: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_time_to_live(Duration::from_secs(2))
            .build()
            .await
            .unwrap();

        let value = cache.get_or_set(1, factory.clone(), None).await;
        assert_eq!(1, value.unwrap());

        tokio::time::sleep(Duration::from_secs(3)).await;

        let cached_value = cache.get(1).await;
        assert_eq!(None, cached_value);
    }

    #[tokio::test]
    async fn test_entry_is_evicted_after_entry_ttl_expires() {
        let factory = TestFactory::new();
        let mut cache: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_time_to_live(Duration::from_secs(10))
            .build()
            .await
            .unwrap();

        let options = FusionCacheOptionsBuilder::new()
            .with_time_to_live(Duration::from_secs(2))
            .build();

        let value = cache.get_or_set(1, factory.clone(), Some(options)).await;
        assert_eq!(1, value.unwrap());

        tokio::time::sleep(Duration::from_secs(3)).await;

        let cached_value = cache.get(1).await;
        assert_eq!(None, cached_value);
    }

    #[derive(Clone, Debug)]
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

    #[derive(Clone, Debug)]
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

    #[derive(Clone, Debug)]
    struct TestFactoryError;

    impl Display for TestFactoryError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestFactoryError")
        }
    }

    impl Into<FusionCacheError> for TestFactoryError {
        fn into(self) -> FusionCacheError {
            FusionCacheError::Other
        }
    }

    #[derive(Clone, Debug)]
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
                None,
            )
            .build()
            .await
            .unwrap();

        // Test setting and getting a value
        let key = 1;
        let value = cache.get_or_set(key, factory.clone(), None).await.unwrap();
        assert_eq!(value, 1);

        // Test getting the value again (should be cached)
        let value = cache.get_or_set(key, factory.clone(), None).await.unwrap();
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
                None,
            )
            .build()
            .await
            .unwrap();

        let mut cache2: FusionCache<u32, u32> = FusionCacheBuilder::new()
            .with_redis(
                "redis://127.0.0.1/".to_string(),
                "test_distributed_cache_synchronization".to_string(),
                false,
                None,
            )
            .build()
            .await
            .unwrap();

        // Set value in cache1
        let key = 1;
        let value = cache1.get_or_set(key, factory.clone(), None).await.unwrap();
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
                None,
            )
            .build()
            .await
            .unwrap();

        // Set initial value
        let key = 1;
        let value = cache.get_or_set(key, factory.clone(), None).await.unwrap();
        assert_eq!(value, 1);

        // Break the Redis connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.break_connection();
        }

        cache.set(key, 2, None).await;

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
                None,
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
        let value = cache.get_or_set(key, factory.clone(), None).await.unwrap();
        assert_eq!(value, 1);

        // Break the Redis connection
        if let Some(distributed_cache) = &mut cache.distributed_cache {
            distributed_cache.break_connection();
        }

        // Try to get the value (should use failsafe)
        cache.set(key, 2, None).await;

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
    struct TraceIdWrapper<F> {
        inner: F,
    }

    impl<S, N, F> FormatEvent<S, N> for TraceIdWrapper<F>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> format::FormatFields<'a> + 'static,
        F: FormatEvent<S, N>,
    {
        fn format_event(
            &self,
            ctx: &FmtContext<'_, S, N>,
            mut writer: format::Writer<'_>,
            event: &Event<'_>,
        ) -> std::fmt::Result {
            // Get the root span's ID, which we use as the trace_id.
            let trace_id = ctx.lookup_current().and_then(|span| {
                span.scope()
                    .from_root()
                    .next() // The root span in the hierarchy
                    .map(|s| s.id())
            });

            if let Some(id) = trace_id {
                // You can customize the format here, e.g., "[trace_id={...}]"
                write!(writer, "trace_id={} ", id.into_u64())?;
            }

            // Let the inner, default formatter do the rest.
            self.inner.format_event(ctx, writer, event)
        }
    }
}
