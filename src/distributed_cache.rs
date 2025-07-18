use std::{env, marker::PhantomData, sync::Arc, time::Duration};

use chrono::Utc;
use futures::StreamExt;
use redis::{AsyncCommands, Client, RedisError, SetExpiry, SetOptions, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_retry::{
    Retry,
    strategy::{FibonacciBackoff, jitter},
};
use tracing::{debug, error, info};

use crate::{CacheValue, FusionCacheError, LOG_TARGET};

impl From<RedisError> for FusionCacheError {
    fn from(error: RedisError) -> Self {
        FusionCacheError::RedisError(error.to_string())
    }
}

#[derive(Serialize, Deserialize)]
pub struct DistributedCacheValue<TValue> {
    value: TValue,
    entry_ttl: Option<i64>,
    entry_tti: Option<i64>,
    last_write: i64,
}

impl<TValue: Clone + Send + Sync + Serialize + DeserializeOwned + 'static> From<CacheValue<TValue>>
    for DistributedCacheValue<TValue>
{
    fn from(value: CacheValue<TValue>) -> Self {
        DistributedCacheValue {
            value: value.value,
            entry_ttl: value.time_to_live.map(|d| d.as_secs() as i64),
            entry_tti: value.time_to_idle.map(|d| d.as_secs() as i64),
            last_write: Utc::now().timestamp_millis(),
        }
    }
}

impl<TValue: Clone + Send + Sync + Serialize + DeserializeOwned + 'static>
    From<DistributedCacheValue<TValue>> for CacheValue<TValue>
{
    fn from(value: DistributedCacheValue<TValue>) -> Self {
        CacheValue {
            value: value.value,
            time_to_live: value.entry_ttl.map(|d| Duration::from_secs(d as u64)),
            time_to_idle: value.entry_tti.map(|d| Duration::from_secs(d as u64)),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct CacheSynchronizationPayload {
    node_id: String,
    key: String,
}

#[derive(Clone, Debug)]
pub struct RedisConnection {
    redis_connection: MultiplexedConnection,
    should_fail: bool,
}

impl RedisConnection {
    pub fn new(redis_connection: MultiplexedConnection) -> Self {
        Self {
            redis_connection,
            // I know, someone might say that I should use dependency injection here, but that would necessarily require boxing the connection,
            // and I don't think it's worth it considering that all I want to do is return an error.
            should_fail: false,
        }
    }
}

impl RedisConnection {
    async fn get(
        &mut self,
        key: &str,
        application_name: &str,
    ) -> Result<Option<String>, FusionCacheError> {
        if self.should_fail {
            return Err(FusionCacheError::RedisError(
                "Failed to get value".to_string(),
            ));
        }
        self.redis_connection
            .get(&format!("{}:{}", application_name, key))
            .await
            .map_err(FusionCacheError::from)
    }
    async fn set(
        &mut self,
        key: &str,
        value: &str,
        application_name: &str,
        entry_ttl: Option<Duration>,
    ) -> Result<(), FusionCacheError> {
        if self.should_fail {
            return Err(FusionCacheError::RedisError(
                "Failed to set value".to_string(),
            ));
        }
        let namespaced_key = format!("{}:{}", application_name, key);
        let mut set_options = SetOptions::default();
        if let Some(entry_ttl) = entry_ttl {
            set_options = set_options.with_expiration(SetExpiry::EX(entry_ttl.as_secs()));
        }
        self.redis_connection
            .set_options(&namespaced_key, value, set_options)
            .await
            .map_err(FusionCacheError::from)
    }
    async fn del(&mut self, key: &str, application_name: &str) -> Result<bool, FusionCacheError> {
        if self.should_fail {
            return Err(FusionCacheError::RedisError(
                "Failed to delete value".to_string(),
            ));
        }
        self.redis_connection
            .del(&format!("{}:{}", application_name, key))
            .await
            .map_err(FusionCacheError::from)
    }
    async fn publish(&mut self, channel: &str, message: &str) -> Result<(), FusionCacheError> {
        if self.should_fail {
            return Err(FusionCacheError::RedisError(
                "Failed to publish message".to_string(),
            ));
        }
        self.redis_connection
            .publish(channel, message)
            .await
            .map_err(FusionCacheError::from)
    }
}

#[derive(Clone, Debug)]
pub struct DistributedCache<
    TKey: Eq + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    TValue: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
> {
    redis_client: Client,
    eviction_event_sender: mpsc::Sender<TKey>,
    auto_recovery_event_sender: mpsc::Sender<(TKey, i64)>,
    synchronization_task: Option<Arc<Mutex<JoinHandle<()>>>>,
    _auto_recovery_task: Arc<Mutex<JoinHandle<()>>>,
    application_name: String,
    _tkey: PhantomData<TKey>,
    _tvalue: PhantomData<TValue>,
    redis_connection: RedisConnection,
    entry_ttl: Option<Duration>,
    node_id: String,
}

impl<
    TKey: Eq + Send + Sync + Clone + Serialize + DeserializeOwned + Debug + 'static,
    TValue: Clone + Send + Sync + Serialize + DeserializeOwned + Debug + 'static,
> DistributedCache<TKey, TValue>
{
    pub fn new(
        redis_connection: RedisConnection,
        redis_client: Client,
        eviction_event_sender: mpsc::Sender<TKey>,
        application_name: String,
        entry_ttl: Option<Duration>,
    ) -> Self {
        let node_id = if env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            env::var("HOSTNAME").unwrap()
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let (auto_recovery_event_sender, mut auto_recovery_event_receiver) =
            mpsc::channel::<(TKey, i64)>(1000);
        let _event_sender = eviction_event_sender.clone();
        let _redis_connection = redis_connection.clone();
        let _node_id = node_id.clone();
        let _application_name = application_name.clone();
        let auto_recovery_task = tokio::spawn(async move {
            let retry_strategy = FibonacciBackoff::from_millis(1000).map(jitter);
            while let Some((key, failure_timestamp)) = auto_recovery_event_receiver.recv().await {
                let _ = Retry::spawn(retry_strategy.clone(), async || {
                    let serialized_key = serde_json::to_string(&key).unwrap();
                    let mut connection = _redis_connection.clone();
                    let value = connection.get(&serialized_key, &_application_name).await?;
                    if let Some(value) = value {
                        let distributed_cache_value: DistributedCacheValue<TValue> =
                            serde_json::from_str(&value).unwrap();
                        if distributed_cache_value.last_write > failure_timestamp {
                            return Ok(());
                        } else {
                            connection.del(&serialized_key, &_application_name).await?;
                            let json_payload =
                                serde_json::to_string(&CacheSynchronizationPayload {
                                    node_id: _node_id.clone(),
                                    key: serialized_key,
                                })
                                .unwrap();
                            return connection
                                .publish(_application_name.as_str(), &json_payload)
                                .await;
                        }
                    }
                    Ok(())
                })
                .await;
            }
        });

        Self {
            redis_connection,
            redis_client,
            eviction_event_sender,
            application_name,
            synchronization_task: None,
            _auto_recovery_task: Arc::new(Mutex::new(auto_recovery_task)),
            auto_recovery_event_sender,
            node_id,
            _tkey: PhantomData,
            _tvalue: PhantomData,
            entry_ttl,
        }
    }

    pub async fn start_synchronization(&mut self) -> Result<(), RedisError> {
        let mut pubsub = self.redis_client.get_async_pubsub().await?;
        pubsub.subscribe(self.application_name.as_str()).await?;
        let eviction_event_sender = self.eviction_event_sender.clone();
        let node_id = self.node_id.clone();
        self.synchronization_task = Some(Arc::new(Mutex::new(tokio::spawn(async move {
            while let Some(message) = pubsub.on_message().next().await {
                let json_message_payload = message.get_payload::<String>().unwrap();
                let payload: CacheSynchronizationPayload =
                    serde_json::from_str(&json_message_payload).unwrap();
                let deserialized_key: TKey = serde_json::from_str(&payload.key).unwrap();
                if payload.node_id != node_id {
                    eviction_event_sender.send(deserialized_key).await.unwrap();
                }
            }
        }))));
        Ok(())
    }

    #[tracing::instrument(name = "DistributedCache::get", skip(self))]
    pub async fn get(
        &mut self,
        key: &TKey,
    ) -> Result<Option<CacheValue<TValue>>, FusionCacheError> {
        let key_str = serde_json::to_string(key).unwrap();
        let value: Option<String> = self
            .redis_connection
            .get(&key_str, &self.application_name)
            .await?;
        let distributed_cache_value: Option<DistributedCacheValue<TValue>> =
            if let Some(value) = value {
                Some(serde_json::from_str(&value).unwrap())
            } else {
                None
            };
        Ok(distributed_cache_value.map(|v| v.into()))
    }

    #[tracing::instrument(name = "DistributedCache::set", skip(self))]
    pub async fn set(
        &mut self,
        key: &TKey,
        value: &CacheValue<TValue>,
    ) -> Result<(), FusionCacheError> {
        let key_str = serde_json::to_string(key).unwrap();
        let value_str = serde_json::to_string(&DistributedCacheValue::from(value.clone())).unwrap();

        let cache_synchronization_payload = CacheSynchronizationPayload {
            node_id: self.node_id.clone(),
            key: key_str.clone(),
        };
        let json_cache_synchronization_payload =
            serde_json::to_string(&cache_synchronization_payload).unwrap();

        match self
            .redis_connection
            .set(&key_str, &value_str, &self.application_name, self.entry_ttl)
            .await
        {
            Ok(_) => {
                debug!(target: LOG_TARGET, "Successfully set value in distributed cache for key: {:?}. Publishing synchronization payload: {:?}", key, json_cache_synchronization_payload);
                let publish_result = self
                    .redis_connection
                    .publish(
                        self.application_name.as_str(),
                        &json_cache_synchronization_payload,
                    )
                    .await;
                if let Err(e) = publish_result {
                    error!(target: LOG_TARGET, "Failed to publish synchronization payload: {:?}. Kicking off auto-recovery for key: {:?}", e, key);
                    let failure_timestamp = Utc::now().timestamp_millis();
                    let key: TKey = serde_json::from_str(&key_str).unwrap();
                    self.auto_recovery_event_sender
                        .send((key.clone(), failure_timestamp))
                        .await
                        .unwrap();
                    self.eviction_event_sender.send(key).await.unwrap();
                    Err(e)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                self.eviction_event_sender.send(key.clone()).await.unwrap();
                let failure_timestamp = Utc::now().timestamp_millis();
                let key: TKey = serde_json::from_str(&key_str).unwrap();
                self.auto_recovery_event_sender
                    .send((key, failure_timestamp))
                    .await
                    .unwrap();
                Err(e)
            }
        }
    }

    pub async fn evict(&mut self, key: &TKey) {
        let key_str = serde_json::to_string(key).unwrap();
        let cache_synchronization_payload = CacheSynchronizationPayload {
            node_id: self.node_id.clone(),
            key: key_str.clone(),
        };
        let json_cache_synchronization_payload =
            serde_json::to_string(&cache_synchronization_payload).unwrap();
        let publish_result = self
            .redis_connection
            .publish(
                self.application_name.as_str(),
                &json_cache_synchronization_payload,
            )
            .await
            .map_err(FusionCacheError::from);
        if let Err(e) = publish_result {
            error!(target: LOG_TARGET, "Failed to publish synchronization payload: {:?}. Kicking off auto-recovery for key: {:?}", e, key);
            let failure_timestamp = Utc::now().timestamp_millis();
            let key: TKey = serde_json::from_str(&key_str).unwrap();
            self.auto_recovery_event_sender
                .send((key, failure_timestamp))
                .await
                .unwrap();
        }
    }

    pub(crate) fn break_connection(&mut self) {
        self.redis_connection.should_fail = true;
    }

    pub(crate) fn restore_connection(&mut self) {
        self.redis_connection.should_fail = false;
    }
}
mod tests {

    use std::time::Duration;

    use super::*;
    #[tokio::test]
    async fn test_basic_set_get() {
        let redis_client = Client::open("redis://127.0.0.1/").unwrap();
        let inner_redis_connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let redis_connection = RedisConnection::new(inner_redis_connection);
        let (eviction_sender, _) = mpsc::channel(100);

        let mut cache = DistributedCache::<String, String>::new(
            redis_connection,
            redis_client,
            eviction_sender,
            "test_app".to_string(),
            None,
        );

        // Test setting and getting a value
        let key = "test_key".to_string();
        let value = "test_value".to_string();
        let cache_value = CacheValue {
            value: value.clone(),
            time_to_live: None,
            time_to_idle: None,
        };

        cache.set(&key, &cache_value).await.unwrap();
        let retrieved_value = cache.get(&key).await.unwrap();

        assert_eq!(retrieved_value, Some(cache_value));
    }

    #[tokio::test]
    async fn test_synchronization() {
        let redis_client1 = Client::open("redis://127.0.0.1/").unwrap();
        let inner_redis_connection1 = redis_client1
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let (eviction_sender1, eviction_receiver1) = mpsc::channel(100);
        let _eviction_receiver1 = eviction_receiver1;
        let redis_connection1 = RedisConnection::new(inner_redis_connection1);

        let mut cache1 = DistributedCache::<String, String>::new(
            redis_connection1,
            redis_client1,
            eviction_sender1,
            "test_synchronization".to_string(),
            None,
        );

        let redis_client2 = Client::open("redis://127.0.0.1/").unwrap();
        let inner_redis_connection2 = redis_client2
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let redis_connection2 = RedisConnection::new(inner_redis_connection2);
        let (eviction_sender2, mut eviction_receiver2) = mpsc::channel(100);

        let mut cache2 = DistributedCache::<String, String>::new(
            redis_connection2,
            redis_client2,
            eviction_sender2,
            "test_synchronization".to_string(),
            None,
        );

        // Start synchronization for both caches
        cache1.start_synchronization().await.unwrap();
        cache2.start_synchronization().await.unwrap();

        // Set a value in cache1
        let key = "sync_test_key".to_string();
        let value = "sync_test_value".to_string();
        let cache_value = CacheValue {
            value: value.clone(),
            time_to_live: None,
            time_to_idle: None,
        };
        cache1.set(&key, &cache_value).await.unwrap();

        // Wait for synchronization
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify cache2 received the eviction event
        let evicted_key = eviction_receiver2.recv().await.unwrap();
        assert_eq!(evicted_key, key);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let redis_client = Client::open("redis://127.0.0.1/").unwrap();
        let inner_redis_connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let (eviction_sender, _) = mpsc::channel(100);
        let redis_connection = RedisConnection::new(inner_redis_connection);

        let mut cache = DistributedCache::<String, String>::new(
            redis_connection,
            redis_client,
            eviction_sender,
            "test_concurrent_writes".to_string(),
            None,
        );

        let key = "concurrent_key".to_string();
        let value1 = "value1".to_string();
        let cache_value1 = CacheValue {
            value: value1.clone(),
            time_to_live: None,
            time_to_idle: None,
        };
        let value2 = "value2".to_string();
        let cache_value2 = CacheValue {
            value: value2.clone(),
            time_to_live: None,
            time_to_idle: None,
        };
        // First write should succeed
        assert!(cache.set(&key, &cache_value1).await.is_ok());

        assert!(cache.set(&key, &cache_value2).await.is_ok());

        // Verify the value was updated
        let retrieved_value = cache.get(&key).await.unwrap();
        assert_eq!(retrieved_value, Some(cache_value2));
    }

    #[tokio::test]
    async fn test_auto_recovery() {
        let redis_client = Client::open("redis://127.0.0.1/").unwrap();
        let internal_redis_connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let working_redis_connection = RedisConnection::new(internal_redis_connection);

        let (eviction_sender, eviction_receiver) = mpsc::channel(100);
        let _eviction_receiver = eviction_receiver;

        let mut cache = DistributedCache::<String, String>::new(
            working_redis_connection,
            redis_client,
            eviction_sender,
            "test_auto_recovery".to_string(),
            None,
        );

        let key = "key".to_string();
        let value = "value".to_string();
        let cache_value = CacheValue {
            value: value.clone(),
            time_to_live: None,
            time_to_idle: None,
        };

        // Set initial value
        cache.set(&key, &cache_value).await.unwrap();

        cache.break_connection();

        let value2 = "value2".to_string();
        let cache_value2 = CacheValue {
            value: value2.clone(),
            time_to_live: None,
            time_to_idle: None,
        };
        let set_result = cache.set(&key, &cache_value2).await;
        assert!(set_result.is_err());

        tokio::time::sleep(Duration::from_secs(2)).await;

        cache.restore_connection();

        // Wait for auto-recovery to kick in
        tokio::time::sleep(Duration::from_secs(2)).await;

        //Verify that the value has been evicted
        let retrieved_value = cache.get(&key).await.unwrap();
        assert_eq!(retrieved_value, None);
    }
}
