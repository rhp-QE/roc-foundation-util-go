package kafka

import (
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/mq"
)

// ProducerConfig Kafka 生产者配置
type ProducerConfig struct {
	// Brokers Kafka 服务器地址
	Brokers []string

	// Acks 确认模式（0=不等待, 1=leader确认, -1=所有副本确认）
	Acks int

	// Compression 压缩类型（none/gzip/snappy/lz4/zstd）
	Compression string

	// MaxRetry 最大重试次数
	MaxRetry int

	// BatchSize 批量大小
	BatchSize int

	// Linger 批量延迟时间
	Linger time.Duration

	// Timeout 超时时间
	Timeout time.Duration

	// EnableIdempotence 是否启用幂等性
	EnableIdempotence bool
}

// DefaultProducerConfig 默认生产者配置
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Brokers:           []string{"localhost:9092"},
		Acks:              1,
		Compression:       "snappy",
		MaxRetry:          3,
		BatchSize:         16384,
		Linger:            10 * time.Millisecond,
		Timeout:           10 * time.Second,
		EnableIdempotence: false,
	}
}

// ProducerOption 生产者配置选项
type ProducerOption func(*ProducerConfig)

// WithProducerBrokers 设置 Broker 地址
func WithProducerBrokers(brokers []string) ProducerOption {
	return func(c *ProducerConfig) {
		c.Brokers = brokers
	}
}

// WithAcks 设置确认模式
func WithAcks(acks int) ProducerOption {
	return func(c *ProducerConfig) {
		c.Acks = acks
	}
}

// WithCompression 设置压缩类型
func WithCompression(compression string) ProducerOption {
	return func(c *ProducerConfig) {
		c.Compression = compression
	}
}

// WithIdempotence 设置是否启用幂等性
func WithIdempotence(enabled bool) ProducerOption {
	return func(c *ProducerConfig) {
		c.EnableIdempotence = enabled
	}
}

// ConsumerConfig Kafka 消费者配置
type ConsumerConfig struct {
	// Brokers Kafka 服务器地址
	Brokers []string

	// GroupID 消费者组ID
	GroupID string

	// Topics 订阅的主题
	Topics []string

	// AutoCommit 是否自动提交偏移量
	AutoCommit bool

	// AutoCommitInterval 自动提交间隔
	AutoCommitInterval time.Duration

	// InitialOffset 初始偏移量（newest/oldest）
	InitialOffset mq.OffsetPosition

	// SessionTimeout 会话超时时间
	SessionTimeout time.Duration

	// HeartbeatInterval 心跳间隔
	HeartbeatInterval time.Duration

	// MaxPollRecords 单次拉取最大记录数
	MaxPollRecords int
}

// DefaultConsumerConfig 默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Brokers:            []string{"localhost:9092"},
		GroupID:            "default-group",
		Topics:             []string{},
		AutoCommit:         true,
		AutoCommitInterval: 1 * time.Second,
		InitialOffset:      mq.OffsetNewest,
		SessionTimeout:     10 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		MaxPollRecords:     500,
	}
}

// ConsumerOption 消费者配置选项
type ConsumerOption func(*ConsumerConfig)

// WithConsumerBrokers 设置 Broker 地址
func WithConsumerBrokers(brokers []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Brokers = brokers
	}
}

// WithConsumerGroupID 设置消费者组ID
func WithConsumerGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// WithConsumerTopics 设置订阅主题
func WithConsumerTopics(topics []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// WithConsumerAutoCommit 设置自动提交
func WithConsumerAutoCommit(enabled bool, interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
		c.AutoCommitInterval = interval
	}
}

// WithConsumerInitialOffset 设置初始偏移量
func WithConsumerInitialOffset(offset mq.OffsetPosition) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.InitialOffset = offset
	}
}

