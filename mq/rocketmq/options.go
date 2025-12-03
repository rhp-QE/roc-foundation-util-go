package rocketmq

import (
	"time"

	"github.com/roc/roc-foundation-util-go/mq"
)

// ProducerConfig RocketMQ 生产者配置
type ProducerConfig struct {
	// NameServer NameServer 地址
	NameServer []string

	// GroupName 生产者组名
	GroupName string

	// InstanceName 实例名称
	InstanceName string

	// Retry 重试次数
	Retry int

	// SendTimeout 发送超时时间
	SendTimeout time.Duration

	// CompressMsgBodyOverHowmuch 消息体压缩阈值
	CompressMsgBodyOverHowmuch int
}

// DefaultProducerConfig 默认生产者配置
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		NameServer:                 []string{"localhost:9876"},
		GroupName:                  "default-producer-group",
		InstanceName:               "default-instance",
		Retry:                      3,
		SendTimeout:                3 * time.Second,
		CompressMsgBodyOverHowmuch: 4096,
	}
}

// ProducerOption 生产者配置选项
type ProducerOption func(*ProducerConfig)

// WithProducerNameServer 设置 NameServer 地址
func WithProducerNameServer(nameServer []string) ProducerOption {
	return func(c *ProducerConfig) {
		c.NameServer = nameServer
	}
}

// WithProducerGroup 设置生产者组
func WithProducerGroup(groupName string) ProducerOption {
	return func(c *ProducerConfig) {
		c.GroupName = groupName
	}
}

// WithProducerInstance 设置实例名称
func WithProducerInstance(instanceName string) ProducerOption {
	return func(c *ProducerConfig) {
		c.InstanceName = instanceName
	}
}

// WithProducerRetry 设置重试次数
func WithProducerRetry(retry int) ProducerOption {
	return func(c *ProducerConfig) {
		c.Retry = retry
	}
}

// ConsumerConfig RocketMQ 消费者配置
type ConsumerConfig struct {
	// NameServer NameServer 地址
	NameServer []string

	// GroupName 消费者组名
	GroupName string

	// Topics 订阅的主题
	Topics []string

	// MessageModel 消息模式（集群/广播）
	MessageModel MessageModel

	// ConsumeFromWhere 从哪里开始消费
	ConsumeFromWhere mq.OffsetPosition

	// MaxReconsumeTimes 最大重试次数
	MaxReconsumeTimes int32

	// ConsumeTimeout 消费超时时间
	ConsumeTimeout time.Duration
}

// MessageModel 消息模式
type MessageModel string

const (
	// Clustering 集群模式（负载均衡）
	Clustering MessageModel = "clustering"

	// Broadcasting 广播模式（每个消费者都收到）
	Broadcasting MessageModel = "broadcasting"
)

// DefaultConsumerConfig 默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		NameServer:        []string{"localhost:9876"},
		GroupName:         "default-consumer-group",
		Topics:            []string{},
		MessageModel:      Clustering,
		ConsumeFromWhere:  mq.OffsetNewest,
		MaxReconsumeTimes: 16,
		ConsumeTimeout:    15 * time.Minute,
	}
}

// ConsumerOption 消费者配置选项
type ConsumerOption func(*ConsumerConfig)

// WithConsumerNameServer 设置 NameServer 地址
func WithConsumerNameServer(nameServer []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.NameServer = nameServer
	}
}

// WithConsumerGroup 设置消费者组
func WithConsumerGroup(groupName string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupName = groupName
	}
}

// WithConsumerTopics 设置订阅主题
func WithConsumerTopics(topics []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// WithMessageModel 设置消息模式
func WithMessageModel(model MessageModel) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MessageModel = model
	}
}

// WithConsumeFromWhere 设置从哪里开始消费
func WithConsumeFromWhere(offset mq.OffsetPosition) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ConsumeFromWhere = offset
	}
}

