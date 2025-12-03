package mq

import (
	"context"
	"time"
)

// Consumer 消息消费者接口
//
// 定义了消息消费的核心功能，让使用者快速了解所有可用方法。
//
// 使用示例：
//
//	consumer := kafka.NewKafkaConsumer(
//	    []kafka.ConsumerOption{
//	        kafka.WithGroupID("order-service"),
//	        kafka.WithTopics([]string{"orders"}),
//	    },
//	)
//	defer consumer.Close()
//
//	// 订阅并消费消息
//	consumer.Subscribe(ctx, func(msg *mq.Message) error {
//	    // 处理消息
//	    fmt.Printf("Received: %s\n", string(msg.Value))
//	    return nil
//	})
type Consumer interface {
	// Subscribe 订阅主题并消费消息
	// 使用提供的处理函数处理每条消息
	Subscribe(ctx context.Context, handler ConsumeHandler) error

	// Consume 手动消费单条消息
	// 用于更细粒度的控制
	Consume(ctx context.Context) (*Message, error)

	// Commit 提交偏移量
	Commit(ctx context.Context, msg *Message) error

	// CommitAll 提交所有已消费的偏移量
	CommitAll(ctx context.Context) error

	// Seek 定位到指定偏移量
	Seek(topic string, partition int32, offset int64) error

	// Pause 暂停消费
	Pause(topics []string) error

	// Resume 恢复消费
	Resume(topics []string) error

	// GetConfig 获取配置
	GetConfig() *ConsumerConfig

	// SetConfig 更新配置
	SetConfig(config *ConsumerConfig)

	// Close 关闭消费者
	Close() error
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	// GroupID 消费者组ID
	GroupID string

	// Topics 订阅的主题列表
	Topics []string

	// AutoCommit 是否自动提交偏移量
	AutoCommit bool

	// AutoCommitInterval 自动提交间隔
	AutoCommitInterval time.Duration

	// InitialOffset 初始偏移量位置（newest/oldest）
	InitialOffset OffsetPosition

	// MaxRetry 最大重试次数
	MaxRetry int

	// ErrorHandler 错误处理函数
	ErrorHandler ErrorHandler
}

// OffsetPosition 偏移量位置
type OffsetPosition string

const (
	// OffsetNewest 从最新位置开始消费
	OffsetNewest OffsetPosition = "newest"

	// OffsetOldest 从最早位置开始消费
	OffsetOldest OffsetPosition = "oldest"
)

// DefaultConsumerConfig 默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		GroupID:            "default-group",
		Topics:             []string{},
		AutoCommit:         true,
		AutoCommitInterval: 1 * time.Second,
		InitialOffset:      OffsetNewest,
		MaxRetry:           3,
	}
}

// ConsumerOption 消费者配置选项
type ConsumerOption func(*ConsumerConfig)

// WithGroupID 设置消费者组ID
func WithGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// WithTopics 设置订阅主题
func WithTopics(topics []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// WithAutoCommit 设置是否自动提交
func WithAutoCommit(enabled bool, interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
		c.AutoCommitInterval = interval
	}
}

// WithInitialOffset 设置初始偏移量
func WithInitialOffset(offset OffsetPosition) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.InitialOffset = offset
	}
}

// WithErrorHandler 设置错误处理函数
func WithErrorHandler(handler ErrorHandler) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ErrorHandler = handler
	}
}
