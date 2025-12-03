# Cache 模块

缓存抽象层，提供统一的缓存接口，当前实现了 Redis。

## 设计理念

- **接口导向**: `Cache` 接口清晰定义所有方法
- **隐藏实现**: `impl` 结构体对外不暴露
- **易于替换**: 可以轻松切换到 Memcached 等其他实现

## 快速使用

```go
package main

import (
    "context"
    "time"
    
    "github.com/roc/roc-foundation-util-go/cache/redis"
)

func main() {
    // 创建 Redis 缓存
    cache, err := redis.NewRedisCache(
        []redis.Option{
            redis.WithAddress("localhost:6379"),
            redis.WithPassword(""),
            redis.WithDB(0),
        },
    )
    if err != nil {
        panic(err)
    }
    defer cache.Close()
    
    ctx := context.Background()
    
    // 设置缓存
    cache.Set(ctx, "user:1", "Alice", 5*time.Minute)
    
    // 获取缓存
    value, err := cache.Get(ctx, "user:1")
    if err != nil {
        panic(err)
    }
    
    println("Value:", value)
}
```

## Cache 接口

完整支持 Redis 的核心数据结构：

### String 操作
```go
Get(ctx, key) (string, error)
Set(ctx, key, value, ttl) error
MGet(ctx, keys...) ([]interface{}, error)
MSet(ctx, pairs) error
Incr/Decr/IncrBy/DecrBy
```

### List 操作
```go
LPush(ctx, key, values...) (int64, error)
RPush(ctx, key, values...) (int64, error)
LPop/RPop(ctx, key) (string, error)
LRange(ctx, key, start, stop) ([]string, error)
LLen(ctx, key) (int64, error)
LRem(ctx, key, count, value) (int64, error)
```

### Set 操作
```go
SAdd(ctx, key, members...) (int64, error)
SRem(ctx, key, members...) (int64, error)
SMembers(ctx, key) ([]string, error)
SIsMember(ctx, key, member) (bool, error)
SCard(ctx, key) (int64, error)
SUnion/SInter/SDiff(ctx, keys...) ([]string, error)
```

### Sorted Set 操作
```go
ZAdd(ctx, key, members...) (int64, error)
ZRem(ctx, key, members...) (int64, error)
ZRange/ZRevRange(ctx, key, start, stop) ([]string, error)
ZRangeByScore(ctx, key, min, max) ([]string, error)
ZScore(ctx, key, member) (float64, error)
ZCard(ctx, key) (int64, error)
ZIncrBy(ctx, key, increment, member) (float64, error)
```

### Hash 操作
```go
HSet(ctx, key, field, value) error
HGet(ctx, key, field) (string, error)
HMSet(ctx, key, fields) error
HMGet(ctx, key, fields...) ([]interface{}, error)
HGetAll(ctx, key) (map[string]string, error)
HDel(ctx, key, fields...) (int64, error)
HExists(ctx, key, field) (bool, error)
HLen/HKeys/HVals
HIncrBy(ctx, key, field, incr) (int64, error)
```

## 配置选项

### Redis 配置

```go
cache, _ := redis.NewRedisCache(
    []redis.Option{
        redis.WithAddress("localhost:6379"),
        redis.WithPassword("your-password"),
        redis.WithDB(0),
        redis.WithPoolSize(20),
        redis.WithTimeout(5*time.Second, 3*time.Second, 3*time.Second),
    },
)
```

### 通用配置

```go
cache, _ := redis.NewRedisCache(
    []redis.Option{ /* redis options */ },
    cache.WithKeyPrefix("myapp:"),
    cache.WithDefaultTTL(10*time.Minute),
)
```

## 扩展实现

可以轻松实现其他缓存后端：

```go
package memcached

type impl struct {
    client *memcache.Client
    config *cache.Config
}

func NewMemcachedCache(options []Option) (cache.Cache, error) {
    // 实现 Cache 接口
}
```

