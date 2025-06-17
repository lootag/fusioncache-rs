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

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
fusion-cache = "0.1.0"
```

## Cache Stampede Protection

Fusion Cache implements a sophisticated cache stampede protection mechanism. When multiple threads request the same key that isn't in the cache:

1. The first request initiates the factory call
2. Subsequent requests for the same key are coalesced using a broadcast channel
3. All waiting requests receive the result once it's available

```rust
// Example of how request coalescing works
let mut cache = FusionCacheBuilder::new().build();

// These concurrent requests will be coalesced
let handles: Vec<_> = (0..1000).map(|_| {
    let mut cache = cache.clone();
    let factory = factory.clone();
    tokio::spawn(async move {
        // Only one factory call will be made, all requests get the same result
        cache.get_or_set(key, factory).await
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
use fusion_cache::{FusionCacheBuilder, Factory};
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
        )
        .build()
        .await
        .unwrap();

    let value = cache.get_or_set("key".to_string(), MyDataFactory)
        .await
        .unwrap();
}
```

## Advanced Usage

### Distributed Cache Configuration

Configure Redis as a distributed cache backend:

```rust
let cache = FusionCacheBuilder::new()
    .with_redis(
        "redis://127.0.0.1/".to_string(),
        "my_app".to_string(),
        true, // Enable background writes for better performance
    )
    .build()
    .await
    .unwrap();
```

The distributed cache provides:
- Automatic synchronization across multiple instances
- Cache invalidation through Redis pub/sub
- Connection resilience with automatic recovery
- Optional background writes for better performance

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

## Examples

### Basic Usage

```rust
let cache = FusionCacheBuilder::new()
    .build()
    .await
    .unwrap();
let value = cache.get_or_set(key, factory).await?;
```

### With Distributed Cache

```rust
let cache = FusionCacheBuilder::new()
    .with_redis(
        "redis://127.0.0.1/".to_string(),
        "my_app".to_string(),
        true,
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License

## Development

To run the tests:

```bash
cargo test
```

The test suite includes comprehensive tests for:
- Concurrent access
- Fail-safe behavior
- Timeout scenarios
- Background execution
- Error handling
- Distributed cache functionality
- Redis connection resilience
