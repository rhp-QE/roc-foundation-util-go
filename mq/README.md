# MQ 模块

消息队列抽象层，提供统一的生产者和消费者接口，当前实现了 Kafka 和 RocketMQ。

## 设计理念

- **接口导向**: `Producer` 和 `Consumer` 接口清晰定义所有方法
- **隐藏实现**: `producerImpl` 和 `consumerImpl` 对外不暴露
- **易于替换**: 可以轻松切换 Kafka 和 RocketMQ

## 快速使用

### Kafka 生产者

```go
import (
    "github.com/roc/roc-foundation-util-go/mq"
    "github.com/roc/roc-foundation-util-go/mq/kafka"
)

// 创建生产者
producer, _ := kafka.NewKafkaProducer(
    []kafka.ProducerOption{
        kafka.WithProducerBrokers([]string{"localhost:9092"}),
        kafka.WithAcks(-1), // 所有副本确认
    },
)
defer producer.Close()

// 发送消息
result, _ := producer.Send(ctx, &mq.Message{
    Topic: "orders",
    Key:   "order-001",
    Value: []byte(`{"id": "001", "amount": 100}`),
    Headers: map[string]string{
        "source": "api-gateway",
    },
})
```

### Kafka 消费者

```go
import (
    "github.com/roc/roc-foundation-util-go/mq/kafka"
)

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
    // 处理消息
    return nil
})
```

### RocketMQ 生产者

```go
import (
    "github.com/roc/roc-foundation-util-go/mq/rocketmq"
)

// 创建生产者
producer, _ := rocketmq.NewRocketMQProducer(
    []rocketmq.ProducerOption{
        rocketmq.WithProducerNameServer([]string{"localhost:9876"}),
        rocketmq.WithProducerGroup("order-producer"),
    },
)
defer producer.Close()

// 发送消息
result, _ := producer.Send(ctx, &mq.Message{
    Topic: "orders",
    Key:   "order-001",
    Value: []byte(`{"id": "001", "amount": 100}`),
})
```

### RocketMQ 消费者

```go
import (
    "github.com/roc/roc-foundation-util-go/mq/rocketmq"
)

// 创建消费者
consumer, _ := rocketmq.NewRocketMQConsumer(
    []rocketmq.ConsumerOption{
        rocketmq.WithConsumerNameServer([]string{"localhost:9876"}),
        rocketmq.WithConsumerGroup("order-consumer"),
        rocketmq.WithConsumerTopics([]string{"orders"}),
    },
)
defer consumer.Close()

// 订阅并消费
consumer.Subscribe(ctx, func(msg *mq.Message) error {
    fmt.Printf("Received: %s\n", string(msg.Value))
    return nil
})
```

## Producer 接口

```go
type Producer interface {
    // 同步发送消息
    Send(ctx context.Context, msg *Message) (*ProduceResult, error)
    
    // 异步发送消息
    SendAsync(ctx context.Context, msg *Message, callback func(*ProduceResult, error)) error
    
    // 批量发送消息
    SendBatch(ctx context.Context, msgs []*Message) ([]*ProduceResult, error)
    
    // 刷新缓冲区
    Flush(ctx context.Context) error
    
    // 配置管理
    GetConfig() *Config
    SetConfig(config *Config)
    
    // 关闭生产者
    Close() error
}
```

## Consumer 接口

```go
type Consumer interface {
    // 订阅主题并消费消息
    Subscribe(ctx context.Context, handler ConsumeHandler) error
    
    // 手动消费单条消息
    Consume(ctx context.Context) (*Message, error)
    
    // 提交偏移量
    Commit(ctx context.Context, msg *Message) error
    CommitAll(ctx context.Context) error
    
    // 定位偏移量
    Seek(topic string, partition int32, offset int64) error
    
    // 暂停/恢复
    Pause(topics []string) error
    Resume(topics []string) error
    
    // 配置管理
    GetConfig() *ConsumerConfig
    SetConfig(config *ConsumerConfig)
    
    // 关闭消费者
    Close() error
}
```

## 配置说明

### Kafka 配置

```go
// 生产者
producer, _ := kafka.NewKafkaProducer(
    []kafka.ProducerOption{
        kafka.WithProducerBrokers([]string{"localhost:9092"}),
        kafka.WithAcks(-1),              // 所有副本确认
        kafka.WithCompression("snappy"), // 压缩
        kafka.WithIdempotence(true),     // 幂等性
    },
)

// 消费者
consumer, _ := kafka.NewKafkaConsumer(
    []kafka.ConsumerOption{
        kafka.WithConsumerBrokers([]string{"localhost:9092"}),
        kafka.WithConsumerGroupID("my-group"),
        kafka.WithConsumerTopics([]string{"topic1", "topic2"}),
        kafka.WithConsumerAutoCommit(true, 1*time.Second),
        kafka.WithConsumerInitialOffset(mq.OffsetNewest),
    },
)
```

### RocketMQ 配置

```go
// 生产者
producer, _ := rocketmq.NewRocketMQProducer(
    []rocketmq.ProducerOption{
        rocketmq.WithProducerNameServer([]string{"localhost:9876"}),
        rocketmq.WithProducerGroup("my-producer"),
        rocketmq.WithProducerRetry(3),
    },
)

// 消费者
consumer, _ := rocketmq.NewRocketMQConsumer(
    []rocketmq.ConsumerOption{
        rocketmq.WithConsumerNameServer([]string{"localhost:9876"}),
        rocketmq.WithConsumerGroup("my-consumer"),
        rocketmq.WithConsumerTopics([]string{"topic1"}),
        rocketmq.WithMessageModel(rocketmq.Clustering), // 或 Broadcasting
    },
)
```

## 使用场景

### 异步解耦

```go
// 订单服务发送消息
producer.Send(ctx, &mq.Message{
    Topic: "orders",
    Value: orderData,
})

// 库存服务消费消息
consumer.Subscribe(ctx, func(msg *mq.Message) error {
    // 减库存
    return nil
})
```

### 事件驱动

```go
// 用户注册事件
producer.Send(ctx, &mq.Message{
    Topic: "user-events",
    Key:   userID,
    Value: []byte(`{"event": "registered"}`),
})

// 邮件服务、通知服务等订阅
consumer.Subscribe(ctx, func(msg *mq.Message) error {
    // 发送欢迎邮件
    return nil
})
```

### 削峰填谷

```go
// 高峰期快速入队
producer.SendBatch(ctx, messages)

// 消费者慢慢处理
consumer.Subscribe(ctx, processMessage)
```

## 对比

| 特性 | Kafka | RocketMQ |
|------|-------|----------|
| **消费模式** | Pull (拉) | Push (推) |
| **顺序消息** | 分区顺序 | 全局顺序支持 |
| **延迟消息** | 不支持 | ✅ 支持 |
| **事务消息** | ✅ 支持 | ✅ 支持 |
| **消息过滤** | 客户端 | ✅ 服务端 |
| **性能** | 极高 | 高 |
| **运维** | 复杂 | 相对简单 |

## 扩展实现

可以轻松实现其他消息队列：

```go
package rabbitmq

type producerImpl struct {
    channel *amqp.Channel
    config  *mq.Config
}

func NewRabbitMQProducer(options []Option) (mq.Producer, error) {
    // 实现 mq.Producer 接口
}
```

