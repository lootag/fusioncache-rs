# fusioncache-rs

A Rust port of https://github.com/ZiggyCreatures/FusionCache


## Disclaimer :warning:

- This is a work in progress and the API is not yet stable. It aspires to be a full Rust port of the original C# library, but it's most definitely not there yet.
- The C# library is an established, battle-tested piece of software: this is not the case for this Rust port. Use the original library for any mission-critical use case.
- None of the features that you see here are my own idea: [@Jody Donetti](https://github.com/jodydonetti) is to be credited for the original concept.


## Features


- 🚀 **High Performance**: Built on top of [moka](https://github.com/moka-rs/moka), a high-performance concurrent cache
- 🔒 **Thread-Safe**: Fully thread-safe for concurrent access
- 🛡️ **Fail-Safe Mode**: Graceful degradation during failures
  - Configurable TTL for fail-safe entries
  - Maximum fail-safe cycles
  - Automatic fail-safe recovery
- ⏰ **Smart Timeouts**: 
  - Soft timeouts with failsafe fallback
  - Hard timeouts
  - Optional background execution for timeout scenarios
- 🎯 **Single Flight**: Prevents cache stampede by coalescing multiple requests for the same key
- 🔄 **Flexible Factory Pattern**: Easy integration with any data source
- 🌐 **Distributed Cache**: Redis-backed distributed caching with automatic synchronization
  - Automatic cache invalidation across instances
  - Configurable background writes
  - Connection resilience and auto-recovery
- ⚙️ **Per-Entry Options**: Fine-grained control over individual cache entries
- 🕐 **Time-to-Idle Support**: Configurable idle time expiration
- 📊 **Comprehensive Logging**: Built-in tracing support for debugging and monitoring

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
fusioncache-rs = "0.1.2"
```

## Cache Stampede Protection

Fusion Cache implements a sophisticated cache stampede protection mechanism. When multiple threads request the same key that isn't in the cache:

1. The first request initiates the factory call
2. Subsequent requests for the same key are coalesced using a broadcast channel
3. All waiting requests receive the result once it's available

```rust
// Example of how request coalescing works
let mut cache = FusionCacheBuilder::new().build().await.unwrap();

// These concurrent requests will be coalesced
let handles: Vec<_> = (0..1000).map(|_| {
    let mut cache = cache.clone();
    let factory = factory.clone();
    tokio::spawn(async move {
        // Only one factory call will be made, all requests get the same result
        cache.get_or_set(key, factory, None).await
    })
}).collect();
```

Under the hood, this is implemented using:
- A shared mutex-protected map of in-flight requests
- Tokio's broadcast channel for efficient result distribution
- Fine-grained locking for good concurrency

The request coalescing mechanism:
- Prevents redundant database/API calls
- Reduces system load under high concurrency
- Ensures consistent results across concurrent requests
- Adds minimal overhead through efficient channel usage



## Quick Start

```rust
use fusion_cache::{FusionCacheBuilder, Factory, FusionCacheOptionsBuilder};
use std::time::Duration;

#[derive(Clone)]
struct MyDataFactory;

#[async_trait]
impl Factory<String, String, Error> for MyDataFactory {
    async fn get(&self, key: &String) -> Result<String, Error> {
        // Your data fetching logic here
        Ok(format!("Value for {}", key))
    }
}

#[tokio::main]
async fn main() {
    let mut cache = FusionCacheBuilder::new()
        .with_capacity(1000)
        .with_time_to_live(Duration::from_secs(300))
        .with_time_to_idle(Duration::from_secs(60))
        .with_fail_safe(
            Duration::from_secs(300),  // Entry TTL
            Duration::from_secs(60),   // Failsafe TTL
            Some(3),                   // Max failsafe cycles
            Some(Duration::from_secs(1)), // Soft timeout
        )
        .with_hard_timeout(Duration::from_secs(5))
        .with_redis(
            "redis://127.0.0.1/".to_string(),
            "my_app".to_string(),
            false, // Whether to perform Redis writes in background
            Some(Duration::from_secs(300)), // Redis entry TTL
        )
        .build()
        .await
        .unwrap();

    let value = cache.get_or_set("key".to_string(), MyDataFactory, None)
        .await
        .unwrap();
}
```

## Advanced Usage

### Per-Entry Options

You can override cache settings for individual entries using `FusionCacheOptionsBuilder`:

```rust
use fusion_cache::FusionCacheOptionsBuilder;

// Create per-entry options
let options = FusionCacheOptionsBuilder::new()
    .with_time_to_live(Duration::from_secs(60))
    .with_time_to_idle(Duration::from_secs(30))
    .skip_distributed_cache() // Skip Redis for this entry
    .build();

// Use options for specific entry
let value = cache.get_or_set("key".to_string(), factory, Some(options)).await?;

// Set value directly with options
cache.set("key".to_string(), "value".to_string(), Some(options)).await;
```

### Distributed Cache Configuration

Configure Redis as a distributed cache backend:

```rust
let cache = FusionCacheBuilder::new()
    .with_redis(
        "redis://127.0.0.1/".to_string(),
        "my_app".to_string(),
        true, // Enable background writes for better performance
        Some(Duration::from_secs(300)), // Redis entry TTL
    )
    .build()
    .await
    .unwrap();
```

The distributed cache provides:
- **Automatic synchronization across multiple instances**: When you have multiple application instances running, the distributed cache ensures that cache entries are automatically shared and synchronized between all instances. When one instance updates a cache entry, all other instances receive the update through Redis, maintaining consistency across your entire application cluster. This eliminates the need for manual cache coordination and ensures that all users see the same data regardless of which instance serves their request.

- **Cache invalidation through Redis pub/sub**: The distributed cache uses Redis's publish/subscribe mechanism to broadcast cache invalidation events across all connected instances. When an entry is invalidated on one instance, a message is published to a Redis channel that all other instances subscribe to, allowing them to immediately remove the corresponding entry from their local cache. This ensures that cache invalidation is propagated instantly across your entire application cluster, preventing stale data from being served. Additionally, when new values are set in the cache, existing entries with the same key are automatically invalidated across all instances to ensure data consistency.

- **Connection resilience with automatic recovery**: The distributed cache is designed to handle Redis connection failures gracefully. If the connection to Redis is lost, the cache continues to function using only the local cache while attempting to reconnect in the background. Once the connection is restored, the cache automatically resumes distributed operations without requiring application restart. This resilience ensures your application remains functional even during Redis outages or network issues.

- **Optional background writes for better performance**: You can configure the distributed cache to perform Redis writes asynchronously in the background, allowing your application to continue processing requests immediately without waiting for Redis operations to complete. This significantly improves response times for write operations while still maintaining eventual consistency. Background writes are particularly beneficial for high-throughput applications where Redis latency could become a bottleneck.

- **Configurable TTL for Redis entries**: Each cache entry in Redis can have its own time-to-live (TTL) setting, allowing you to control how long data persists in the distributed cache independently of the local cache TTL. This flexibility enables you to optimize Redis storage usage by setting shorter TTLs for frequently changing data while keeping stable data in Redis longer. You can also set different TTLs for different types of data based on their update frequency and importance.

### Fail-Safe Configuration

The fail-safe mechanism provides resilience during failures by maintaining a secondary cache:

```rust
let cache = FusionCacheBuilder::new()
    .with_fail_safe(
        Duration::from_secs(300),  // How long entries live in main cache
        Duration::from_secs(60),   // How long entries live in failsafe cache
        Some(3),                   // Maximum number of failsafe cycles
        Some(Duration::from_secs(1)), // Soft timeout before failsafe kicks in
    )
    .build()
    .await
    .unwrap();
```

### Timeout Behaviors

fusioncache-rs provides sophisticated timeout handling to ensure your application remains responsive even when data sources are slow or unresponsive. The cache implements multiple layers of timeout protection that work together to provide graceful degradation and optimal user experience.

**Soft Timeouts** are the first line of defense against slow operations. When a factory operation takes longer than the configured soft timeout, the cache immediately returns a cached value from the fail-safe cache (if available) while allowing the original operation to continue in the background (if background execution is enabled). This ensures users get a response quickly, even if it's slightly stale data. The soft timeout is particularly useful for operations that are expensive but not critical for real-time accuracy.

**Hard Timeouts** provide an absolute upper bound on how long the cache will wait for a factory operation to complete. Once this timeout is reached, the operation is forcefully terminated and the cache returns an error. This prevents your application from hanging indefinitely when external services are completely unresponsive. Hard timeouts are essential for maintaining system responsiveness and preventing resource exhaustion.

**Background Execution** can be enabled to allow factory operations to continue running even after a soft timeout has occurred. When enabled, the cache will return the vailue from the fail-safe cache immediately (if available), but the factory operation continues executing in the background. If the background operation completes successfully, the result is cached for future requests. This feature is particularly valuable for operations that are expensive to compute but where you want to avoid repeated failures.

The timeout system works in harmony with the fail-safe mechanism: when a soft timeout triggers, the cache falls back to fail-safe data; when a hard timeout triggers, the cache will return an error. This multi-layered approach ensures that your application can gracefully handle various failure scenarios while maintaining optimal performance.

Control how the cache handles slow operations:

```rust
let cache = FusionCacheBuilder::new()
    .with_hard_timeout(Duration::from_secs(5))  // Maximum time to wait
    .set_factory_background_execution(true)      // Continue execution after timeout
    .build()
    .await
    .unwrap();
```

### Factory Pattern

Implement the `Factory` trait for your data source:

```rust
#[async_trait]
impl Factory<KeyType, ValueType, ErrorType> for MyFactory {
    async fn get(&self, key: &KeyType) -> Result<ValueType, ErrorType> {
        // Your implementation here
    }
}
```

### Error Handling

The cache provides detailed error types:

```rust
pub enum FusionCacheError {
    Other,
    SystemCorruption,
    FactoryError,
    FactoryTimeout,
    InitializationError(String),
    RedisError(String),
}
```

## Performance Considerations

- Uses single-flight pattern to prevent cache stampede
- Configurable background execution for timeout scenarios
- Built on proven concurrent cache implementation (moka)
- Thread-safe for high-concurrency environments
- Optional background Redis writes for better performance
- Efficient distributed cache synchronization
- Per-entry option overrides for fine-grained control
- Time-to-idle expiration for better cache efficiency

## Examples

### Basic Usage

```rust
let cache = FusionCacheBuilder::new()
    .build()
    .await
    .unwrap();
let value = cache.get_or_set(key, factory, None).await?;
```

### With Distributed Cache

```rust
let cache = FusionCacheBuilder::new()
    .with_redis(
        "redis://127.0.0.1/".to_string(),
        "my_app".to_string(),
        true,
        Some(Duration::from_secs(300)),
    )
    .build()
    .await
    .unwrap();
```

### With Fail-Safe and Timeouts

```rust
let cache = FusionCacheBuilder::new()
    .with_fail_safe(
        Duration::from_secs(300),
        Duration::from_secs(60),
        Some(3),
        Some(Duration::from_secs(1)),
    )
    .with_hard_timeout(Duration::from_secs(5))
    .set_factory_background_execution(true)
    .build()
    .await
    .unwrap();
```

### With Per-Entry Options

```rust
// Create cache with default settings
let mut cache = FusionCacheBuilder::new()
    .with_time_to_live(Duration::from_secs(300))
    .build()
    .await
    .unwrap();

// Override settings for specific entry
let options = FusionCacheOptionsBuilder::new()
    .with_time_to_live(Duration::from_secs(60))
    .skip_distributed_cache()
    .build();

let value = cache.get_or_set("key", factory, Some(options)).await?;
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License

## Development

To run the tests:

```bash
cargo test
```

NB: you need to have a local redis instance running on port 6379 for the tests to pass.

The test suite includes comprehensive tests for:
- Concurrent access
- Fail-safe behavior
- Timeout scenarios
- Background execution
- Error handling
- Distributed cache functionality
- Redis connection resilience
- Per-entry options and overrides
- Time-to-idle expiration
- Cache stampede protection
