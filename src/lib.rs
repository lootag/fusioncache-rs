use moka::future::Cache;
use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc};
use tokio::sync::{Mutex, broadcast};

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

    pub fn with_fail_safe(mut self, entry_ttl: u64, failsafe_ttl: u64) -> Self {
        self.fail_safe_configuration = Some(FailSafeConfiguration {
            entry_ttl,
            failsafe_ttl,
        });
        self
    }

    pub fn build<F: Factory<TKey, TValue, TError>>(
        self,
        factory: F,
    ) -> FusionCache<TKey, TValue, TError, F> {
        FusionCache {
            cache: Cache::new(self.capacity),
            fail_safe_configuration: self.fail_safe_configuration,
            fail_safe_cache: Cache::new(self.capacity),
            factory,
            bookkeeper: Arc::new(Mutex::new(HashMap::new())),
            phantom_t_error: PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum FusionCacheError {
    Other,
    SystemCorruption,
}

#[derive(Clone)]
pub struct FusionCache<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + Clone + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
    F: Factory<TKey, TValue, TError>,
> {
    cache: Cache<TKey, TValue>,
    fail_safe_configuration: Option<FailSafeConfiguration>,
    fail_safe_cache: Cache<TKey, TValue>,
    factory: F,
    bookkeeper: Arc<Mutex<HashMap<TKey, broadcast::Sender<TValue>>>>,
    phantom_t_error: PhantomData<TError>,
}

impl<
    TKey: Hash + Eq + Send + Sync + Clone + 'static,
    TValue: Clone + Send + Sync + Clone + 'static,
    TError: Clone + Send + Sync + Into<FusionCacheError> + 'static,
    F: Factory<TKey, TValue, TError>,
> FusionCache<TKey, TValue, TError, F>
{
    pub async fn get_or_set(&self, key: TKey) -> Result<TValue, FusionCacheError> {
        let mut bookkeeper = self.bookkeeper.lock().await;
        let sender = bookkeeper.get(&key).cloned();
        let maybe_entry = self.cache.get(&key).await;
        match maybe_entry {
            Some(entry) => {
                if let Some(_) = sender {
                    bookkeeper.remove(&key);
                }
                drop(bookkeeper);
                Ok(entry)
            }
            None => match sender {
                Some(sender) => {
                    drop(bookkeeper);
                    let mut receiver = sender.subscribe();
                    let value = receiver.recv().await;
                    if let Ok(value) = value {
                        Ok(value)
                    } else {
                        let maybe_value = self.cache.get(&key).await;
                        match maybe_value {
                            Some(value) => Ok(value),
                            None => Err(FusionCacheError::SystemCorruption),
                        }
                    }
                }
                None => {
                    let (tx, _) = broadcast::channel(1);
                    bookkeeper.insert(key.clone(), tx.clone());
                    drop(bookkeeper);
                    let value = self.factory.get(&key).await.map_err(|e| e.into())?;
                    self.cache.insert(key.clone(), value.clone()).await;
                    let _ = tx.send(value.clone());
                    Ok(value)
                }
            },
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
}

#[derive(Clone)]
struct FailSafeConfiguration {
    entry_ttl: u64,
    failsafe_ttl: u64,
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
            let cache = cache.clone();
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
