# roc-foundation-util-go

通用基础工具库，为微服务开发提供核心基础能力。

## 📦 模块概览

| 模块 | 功能 | 当前实现 | 接口完善度 |
|------|------|----------|-----------|
| **service_registry** | 服务注册与发现 | etcd | ⭐⭐⭐⭐⭐ |
| **cache** | 缓存抽象层 | Redis | ⭐⭐⭐⭐⭐ |
| **storage** | 存储抽象层 | MongoDB | ⭐⭐⭐⭐⭐ |
| **mq** | 消息队列抽象层 | Kafka + RocketMQ | ⭐⭐⭐⭐⭐ |

---

## 🎯 设计理念

### 接口导向
所有模块都通过接口暴露，便于替换实现：
- ✅ **Registry** - 可替换为 Consul、Nacos 等
- ✅ **LoadBalancer** - 可替换为自定义策略
- ✅ **Cache** - 可替换为 Memcached 等
- ✅ **Storage** - 可替换为 PostgreSQL 等

### 隐藏实现
内部实现（`impl`）对用户不可见，保持良好的封装性。

### 统一错误
每个模块定义统一的错误类型，屏蔽底层差异。

---

### 4. MQ - 消息队列

```go
import (
    "github.com/roc/roc-foundation-util-go/mq"
    "github.com/roc/roc-foundation-util-go/mq/kafka"
)

// 创建 Kafka 生产者
producer, _ := kafka.NewKafkaProducer(
    []kafka.ProducerOption{
        kafka.WithProducerBrokers([]string{"localhost:9092"}),
    },
)
defer producer.Close()

// 发送消息
result, _ := producer.Send(ctx, &mq.Message{
    Topic: "orders",
    Key:   "order-001",
    Value: []byte(`{"id": "001", "amount": 100}`),
})

// 创建消费者
consumer, _ := kafka.NewKafkaConsumer(
    []kafka.ConsumerOption{
        kafka.WithConsumerBrokers([]string{"localhost:9092"}),
        kafka.WithConsumerGroupID("order-service"),
        kafka.WithConsumerTopics([]string{"orders"}),
    },
)
defer consumer.Close()

// 订阅并消费
consumer.Subscribe(ctx, func(msg *mq.Message) error {
    fmt.Printf("Received: %s\n", string(msg.Value))
    return nil
})
```

**支持的实现**：
- ✅ Kafka - 高吞吐量、分布式流处理平台
- ✅ RocketMQ - 低延迟、高可靠性消息队列

---

## 🚀 快速开始

### 1. Service Registry - 服务注册与发现

```go
import (
    "github.com/roc/roc-foundation-util-go/service_registry/registry/etcd"
    "github.com/roc/roc-foundation-util-go/service_registry/discovery"
    "github.com/roc/roc-foundation-util-go/service_registry/loadbalancer"
)

// 创建注册中心
reg, _ := etcd.NewEtcdRegistry(
    etcd.WithEndpoints([]string{"localhost:2379"}),
)
defer reg.Close()

// 注册服务
instance := &registry.ServiceInstance{
    ServiceName: "user-service",
    InstanceID:  "user-001",
    Host:        "127.0.0.1",
    Port:        8080,
    Status:      registry.StatusHealthy,
}
reg.Register(ctx, instance)

// 服务发现
lb := loadbalancer.NewRoundRobinLoadBalancer()
disc := discovery.NewDiscovery(reg, lb)
instance, _ := disc.GetInstance(ctx, "user-service")
```

**负载均衡策略**：
- 轮询 (RoundRobin)
- 随机 (Random)
- 加权随机 (WeightedRandom)
- 一致性哈希 (ConsistentHash)

---

### 2. Cache - 缓存抽象层

```go
import (
    "github.com/roc/roc-foundation-util-go/cache/redis"
    "github.com/roc/roc-foundation-util-go/cache"
)

// 创建 Redis 缓存
c, _ := redis.NewRedisCache(
    []redis.Option{
        redis.WithAddress("localhost:6379"),
        redis.WithPassword(""),
    },
    cache.WithKeyPrefix("myapp:"),
)
defer c.Close()

// String 操作
c.Set(ctx, "key", "value", 5*time.Minute)
value, _ := c.Get(ctx, "key")

// List 操作（队列）
c.RPush(ctx, "queue", "task1", "task2")
task, _ := c.LPop(ctx, "queue")

// Set 操作（集合）
c.SAdd(ctx, "tags", "go", "redis")
tags, _ := c.SMembers(ctx, "tags")

// Sorted Set 操作（排行榜）
c.ZAdd(ctx, "leaderboard", 
    cache.ZMember{Score: 100, Member: "Alice"})
top10, _ := c.ZRevRange(ctx, "leaderboard", 0, 9)

// Hash 操作（对象缓存）
c.HSet(ctx, "user:1", "name", "Alice")
c.HSet(ctx, "user:1", "age", "30")
userInfo, _ := c.HGetAll(ctx, "user:1")
```

**支持的数据结构**：
- ✅ String - 基本键值、计数器
- ✅ List - 队列、栈
- ✅ Set - 集合、标签
- ✅ Sorted Set - 排行榜、优先队列
- ✅ Hash - 对象存储、购物车

---

### 3. Storage - 存储抽象层

```go
import (
    "github.com/roc/roc-foundation-util-go/storage/mongodb"
    "github.com/roc/roc-foundation-util-go/storage"
    "go.mongodb.org/mongo-driver/bson"
)

type User struct {
    ID    string `bson:"_id"`
    Name  string `bson:"name"`
    Email string `bson:"email"`
}

// 创建 MongoDB 存储
store, _ := mongodb.NewMongoStorage(
    []mongodb.Option{
        mongodb.WithURI("mongodb://localhost:27017"),
        mongodb.WithDatabase("mydb"),
    },
)
defer store.Close()

// 插入文档
user := User{ID: "1", Name: "Alice", Email: "alice@example.com"}
store.InsertOne(ctx, "users", user)

// 查询文档
var result User
filter := mongodb.BuildIDFilter("1")
store.FindOne(ctx, "users", filter, &result)

// 更新文档
update := mongodb.BuildUpdateSet(bson.M{"name": "Alice Wang"})
store.UpdateOne(ctx, "users", filter, update)

// 原子操作（查找并更新）
var updated User
store.FindOneAndUpdate(ctx, "users", filter, update, &updated)

// 聚合查询
pipeline := []bson.M{
    {"$group": bson.M{"_id": "$category", "count": bson.M{"$sum": 1}}},
}
var results []interface{}
store.Aggregate(ctx, "products", pipeline, &results)

// 批量操作
operations := []storage.BulkOperation{
    {Type: storage.BulkInsert, Document: user1},
    {Type: storage.BulkUpdate, Filter: filter, Update: update},
}
store.BulkWrite(ctx, "users", operations)
```

**支持的操作**：
- ✅ CRUD - InsertOne/Find/UpdateOne/DeleteOne
- ✅ 批量操作 - InsertMany/BulkWrite
- ✅ 原子操作 - FindOneAndUpdate/Delete/Replace
- ✅ 聚合查询 - Aggregate
- ✅ 索引管理 - CreateIndex/DropIndex
- ✅ 统计分析 - Count/Distinct

---

## 📁 项目结构

```
roc-foundation-util-go/
├── service_registry/        # 服务注册中心
│   ├── registry/            # 注册中心接口和 etcd 实现
│   ├── discovery/           # 服务发现客户端
│   ├── loadbalancer/        # 负载均衡器
│   └── examples/            # 示例代码
├── cache/                   # 缓存抽象层
│   ├── cache.go             # Cache 接口
│   ├── errors.go            # 错误定义
│   └── redis/               # Redis 实现及测试
├── storage/                 # 存储抽象层
│   ├── storage.go           # Storage 接口
│   ├── errors.go            # 错误定义
│   └── mongodb/             # MongoDB 实现及测试
├── mq/                      # 消息队列抽象层
│   ├── producer.go          # Producer 接口
│   ├── consumer.go          # Consumer 接口
│   ├── types.go             # 消息类型
│   ├── errors.go            # 错误定义
│   ├── kafka/               # Kafka 实现及测试
│   └── rocketmq/            # RocketMQ 实现及测试
├── go.mod
├── go.sum
└── README.md
```

---

## 🧪 运行测试

### 启动依赖服务

```bash
# etcd
docker run -d --name etcd -p 2379:2379 \
  quay.io/coreos/etcd:latest \
  /usr/local/bin/etcd \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379

# Redis
docker run -d --name redis -p 6379:6379 \
  -e REDIS_PASSWORD=redis123 redis:latest \
  --requirepass redis123

# MongoDB
docker run -d --name mongodb -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=mongodb123 \
  mongo:latest

# Kafka
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest

# RocketMQ
docker run -d --name rocketmq -p 9876:9876 -p 10911:10911 \
  apache/rocketmq:latest
```

### 运行测试

```bash
# 所有模块
go test ./...

# 特定模块
go test ./service_registry/... -v
go test ./cache/redis -v
go test ./storage/mongodb -v
go test ./mq/kafka -v
go test ./mq/rocketmq -v

# 只测试负载均衡器（不需要外部依赖）
go test ./service_registry/loadbalancer -v
```

---

## 🔧 安装

```bash
go get github.com/roc/roc-foundation-util-go
```

---

## 📖 详细文档

- [Service Registry 详细文档](service_registry/README.md)
- [Service Registry Discovery](service_registry/discovery/README.md)
- [Cache 详细文档](cache/README.md)
- [Storage 详细文档](storage/README.md)
- [MQ 详细文档](mq/README.md)

---

## 🌟 核心特性

### 1. 完整的接口定义
每个模块都有清晰的接口定义，用户一眼就能看出所有可用方法。

### 2. 多种实现支持
- **Registry**: etcd（可扩展 Consul、Nacos）
- **LoadBalancer**: 4种策略（可扩展自定义）
- **Cache**: Redis（可扩展 Memcached）
- **Storage**: MongoDB（可扩展 PostgreSQL）

### 3. 统一的错误处理
每个模块定义统一的错误类型，屏蔽底层实现差异。

### 4. 生产就绪
- ✅ 自动心跳保活
- ✅ 连接池管理
- ✅ 健康检查
- ✅ 完整的测试覆盖

### 5. 易于测试
基于接口设计，方便 mock 和单元测试。

---

## 💡 使用场景

### 微服务架构
```go
// 服务注册
reg.Register(ctx, serviceInstance)

// 服务发现 + 负载均衡
disc.GetInstance(ctx, "user-service")

// 缓存用户信息
cache.HSet(ctx, "user:1", "name", "Alice")

// 存储用户数据
storage.InsertOne(ctx, "users", user)
```

### 分布式系统
```go
// 一致性哈希（会话保持）
lb := loadbalancer.NewConsistentHashLoadBalancer(150)
instance, _ := disc.GetInstance(ctx, "session-service", userID)

// 分布式计数器
cache.Incr(ctx, "global:counter")

// 原子操作
storage.FindOneAndUpdate(ctx, "inventory", filter, update, &result)
```

---

## 🎓 最佳实践

### 1. 优雅退出
```go
// 使用 defer 确保资源释放
defer reg.Close()
defer cache.Close()
defer storage.Close()
```

### 2. 错误处理
```go
if err == cache.ErrKeyNotFound {
    // 处理缓存未命中
}
if err == storage.ErrNotFound {
    // 处理文档不存在
}
```

### 3. 配置管理
```go
// 运行时调整配置
config := disc.GetConfig()
config.CacheTTL = 60 * time.Second
disc.SetConfig(config)
```

---

## 🔮 扩展指南

### 实现新的注册中心

```go
package consul

type impl struct {
    client *consul.Client
    config *registry.Config
}

func NewConsulRegistry(options ...Option) (registry.Registry, error) {
    // 实现 registry.Registry 接口
}
```

### 实现新的缓存后端

```go
package memcached

type impl struct {
    client *memcache.Client
    config *cache.Config
}

func NewMemcachedCache(options ...Option) (cache.Cache, error) {
    // 实现 cache.Cache 接口
}
```

---

## 📊 接口完善度

| 模块 | 基础功能 | 数据结构 | 原子操作 | 总体 |
|------|---------|---------|---------|------|
| **service_registry** | ✅ 100% | ✅ N/A | ✅ 100% | **⭐⭐⭐⭐⭐** |
| **cache** | ✅ 100% | ✅ 100% | ✅ 100% | **⭐⭐⭐⭐⭐** |
| **storage** | ✅ 100% | ✅ N/A | ✅ 100% | **⭐⭐⭐⭐⭐** |

### Cache 接口覆盖
- ✅ String (Get/Set/Incr/Decr)
- ✅ List (LPush/RPush/LPop/RPop/LRange)
- ✅ Set (SAdd/SMembers/SUnion/SInter)
- ✅ Sorted Set (ZAdd/ZRange/ZScore)
- ✅ Hash (HSet/HGet/HGetAll)

### Storage 接口覆盖
- ✅ CRUD (Insert/Find/Update/Delete)
- ✅ 批量操作 (InsertMany/BulkWrite)
- ✅ 原子操作 (FindOneAndUpdate/Delete)
- ✅ 聚合查询 (Aggregate/Distinct)
- ✅ 索引管理 (CreateIndex/ListIndexes)

---

## 📝 示例代码

### 完整微服务示例

```go
package main

import (
    "context"
    "log"
    "time"
    
    // 服务注册
    "github.com/roc/roc-foundation-util-go/service_registry/registry/etcd"
    "github.com/roc/roc-foundation-util-go/service_registry/discovery"
    "github.com/roc/roc-foundation-util-go/service_registry/loadbalancer"
    
    // 缓存
    "github.com/roc/roc-foundation-util-go/cache/redis"
    
    // 存储
    "github.com/roc/roc-foundation-util-go/storage/mongodb"
)

func main() {
    ctx := context.Background()
    
    // 1. 服务注册
    reg, _ := etcd.NewEtcdRegistry(
        etcd.WithEndpoints([]string{"localhost:2379"}),
    )
    defer reg.Close()
    
    // 2. 服务发现
    lb := loadbalancer.NewRoundRobinLoadBalancer()
    disc := discovery.NewDiscovery(reg, lb)
    defer disc.Close()
    
    // 3. Redis 缓存
    cache, _ := redis.NewRedisCache(
        []redis.Option{redis.WithAddress("localhost:6379")},
    )
    defer cache.Close()
    
    // 4. MongoDB 存储
    store, _ := mongodb.NewMongoStorage(
        []mongodb.Option{
            mongodb.WithURI("mongodb://localhost:27017"),
            mongodb.WithDatabase("mydb"),
        },
    )
    defer store.Close()
    
    // 使用服务
    instance, _ := disc.GetInstance(ctx, "user-service")
    log.Printf("Using instance: %s:%d", instance.Host, instance.Port)
    
    // 缓存用户会话
    cache.Set(ctx, "session:"+userID, sessionData, 30*time.Minute)
    
    // 存储用户数据
    store.InsertOne(ctx, "users", userData)
}
```

---

## 🧰 工具函数

### MongoDB Helper

```go
import "github.com/roc/roc-foundation-util-go/storage/mongodb"

// 构建过滤器
filter := mongodb.BuildIDFilter("user-001")

// 构建更新操作
update := mongodb.BuildUpdateSet(bson.M{"age": 31})
incUpdate := mongodb.BuildUpdateInc(bson.M{"score": 10})

// 构建排序
sort := mongodb.BuildSortDesc("created_at")
```

---

## 📚 各模块详细功能

### Service Registry
- 服务注册/注销
- 服务发现/订阅
- 心跳保活
- 负载均衡（4种策略）
- 实例缓存
- 实时监听

### Cache
- String 操作（Get/Set/Incr）
- List 操作（队列、栈）
- Set 操作（集合运算）
- Sorted Set 操作（排行榜）
- Hash 操作（对象缓存）
- 批量操作（MGet/MSet）
- 过期时间管理

### Storage
- 基本 CRUD
- 批量操作（BulkWrite）
- 原子操作（FindAndModify）
- 聚合查询（Pipeline）
- 索引管理
- 分页排序

---

## ⚙️ 配置说明

所有模块都支持灵活的配置：

```go
// 创建时配置
cache, _ := redis.NewRedisCache(
    []redis.Option{
        redis.WithAddress("localhost:6379"),
        redis.WithPoolSize(20),
    },
    cache.WithKeyPrefix("app:"),
    cache.WithDefaultTTL(10*time.Minute),
)

// 运行时配置
config := cache.GetConfig()
config.KeyPrefix = "newapp:"
cache.SetConfig(config)
```

---

## 🛡️ 错误处理

### 统一的错误定义

```go
// Cache 错误
if err == cache.ErrKeyNotFound {
    // 缓存未命中
}

// Storage 错误
if err == storage.ErrNotFound {
    // 文档不存在
}
if err == storage.ErrDuplicateKey {
    // 唯一键冲突
}
```

---

## 🔬 测试覆盖

```bash
✅ service_registry/discovery    - 6个测试
✅ service_registry/loadbalancer - 8个测试
✅ service_registry/registry/etcd - 5个测试
✅ cache/redis                   - 7个测试
✅ storage/mongodb               - 7个测试
```

---

## 🚧 未来规划

- [ ] 更多注册中心实现（Consul、Nacos）
- [ ] 更多缓存实现（Memcached）
- [ ] 更多存储实现（PostgreSQL、MySQL）
- [ ] 配置中心模块
- [ ] 分布式锁模块
- [ ] 消息队列模块
- [ ] 监控指标收集

---

## 📄 License

MIT License
