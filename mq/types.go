package mq

import "time"

// Message 消息结构
type Message struct {
	// Topic 主题
	Topic string

	// Key 消息键（用于分区）
	Key string

	// Value 消息内容
	Value []byte

	// Headers 消息头
	Headers map[string]string

	// Timestamp 消息时间戳
	Timestamp time.Time

	// Partition 分区（可选，由系统分配）
	Partition int32

	// Offset 偏移量（消费时有效）
	Offset int64
}

// ProduceResult 生产结果
type ProduceResult struct {
	// MessageID 消息ID
	MessageID string

	// Topic 主题
	Topic string

	// Partition 分区
	Partition int32

	// Offset 偏移量
	Offset int64

	// Timestamp 时间戳
	Timestamp time.Time
}

// ConsumeHandler 消费处理函数
type ConsumeHandler func(msg *Message) error

// ErrorHandler 错误处理函数
type ErrorHandler func(err error)

// Config 通用配置
type Config struct {
	// Brokers 服务器地址列表
	Brokers []string

	// ClientID 客户端ID
	ClientID string

	// EnableMetrics 是否启用监控
	EnableMetrics bool

	// EnableTracing 是否启用追踪
	EnableTracing bool

	// RetryMax 最大重试次数
	RetryMax int

	// Timeout 操作超时时间
	Timeout time.Duration
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Brokers:       []string{"localhost:9092"},
		ClientID:      "mq-client",
		EnableMetrics: false,
		EnableTracing: false,
		RetryMax:      3,
		Timeout:       10 * time.Second,
	}
}

// Option 配置选项
type Option func(*Config)

// WithBrokers 设置服务器地址
func WithBrokers(brokers []string) Option {
	return func(c *Config) {
		c.Brokers = brokers
	}
}

// WithClientID 设置客户端ID
func WithClientID(clientID string) Option {
	return func(c *Config) {
		c.ClientID = clientID
	}
}

// WithMetrics 设置是否启用监控
func WithMetrics(enabled bool) Option {
	return func(c *Config) {
		c.EnableMetrics = enabled
	}
}

// WithRetry 设置重试次数
func WithRetry(max int) Option {
	return func(c *Config) {
		c.RetryMax = max
	}
}

// WithTimeout 设置超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

