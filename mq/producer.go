package mq

import "context"

// Producer 消息生产者接口
//
// 定义了消息生产的核心功能，让使用者快速了解所有可用方法。
//
// 使用示例：
//
//	producer := kafka.NewKafkaProducer(
//	    []kafka.Option{
//	        kafka.WithBrokers([]string{"localhost:9092"}),
//	    },
//	)
//	defer producer.Close()
//
//	// 发送消息
//	result, err := producer.Send(ctx, &mq.Message{
//	    Topic: "orders",
//	    Key:   "order-001",
//	    Value: []byte("order data"),
//	})
type Producer interface {
	// Send 同步发送消息
	Send(ctx context.Context, msg *Message) (*ProduceResult, error)

	// SendAsync 异步发送消息
	SendAsync(ctx context.Context, msg *Message, callback func(*ProduceResult, error)) error

	// SendBatch 批量发送消息
	SendBatch(ctx context.Context, msgs []*Message) ([]*ProduceResult, error)

	// Flush 刷新缓冲区，确保所有消息都已发送
	Flush(ctx context.Context) error

	// GetConfig 获取配置
	GetConfig() *Config

	// SetConfig 更新配置
	SetConfig(config *Config)

	// Close 关闭生产者
	Close() error
}

