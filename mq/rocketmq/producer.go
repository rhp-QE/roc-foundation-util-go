package rocketmq

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/rhp-QE/roc-foundation-util-go/mq"
)

// producerImpl RocketMQ 生产者的内部实现
type producerImpl struct {
	producer rocketmq.Producer
	config   *mq.Config
}

// NewRocketMQProducer 创建 RocketMQ 生产者
//
// 参数：
//   - options: RocketMQ 生产者配置选项
//   - mqOptions: MQ 通用配置选项
//
// 返回：
//   - mq.Producer 接口实例
func NewRocketMQProducer(options []ProducerOption, mqOptions ...mq.Option) (mq.Producer, error) {
	// RocketMQ 配置
	rmqConfig := DefaultProducerConfig()
	for _, opt := range options {
		opt(rmqConfig)
	}

	// MQ 通用配置
	mqConfig := mq.DefaultConfig()
	for _, opt := range mqOptions {
		opt(mqConfig)
	}

	// 创建生产者
	p, err := rocketmq.NewProducer(
		producer.WithNameServer(rmqConfig.NameServer),
		producer.WithGroupName(rmqConfig.GroupName),
		producer.WithInstanceName(rmqConfig.InstanceName),
		producer.WithRetry(rmqConfig.Retry),
		producer.WithSendMsgTimeout(rmqConfig.SendTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create rocketmq producer: %w", err)
	}

	// 启动生产者
	if err := p.Start(); err != nil {
		return nil, fmt.Errorf("failed to start rocketmq producer: %w", err)
	}

	return &producerImpl{
		producer: p,
		config:   mqConfig,
	}, nil
}

// Send 同步发送消息
func (p *producerImpl) Send(ctx context.Context, msg *mq.Message) (*mq.ProduceResult, error) {
	if msg == nil || msg.Topic == "" {
		return nil, mq.ErrInvalidMessage
	}

	// 创建 RocketMQ 消息
	rmqMsg := primitive.NewMessage(msg.Topic, msg.Value)
	if msg.Key != "" {
		rmqMsg.WithKeys([]string{msg.Key})
	}

	// 设置消息属性
	if len(msg.Headers) > 0 {
		for k, v := range msg.Headers {
			rmqMsg.WithProperty(k, v)
		}
	}

	// 发送消息
	result, err := p.producer.SendSync(ctx, rmqMsg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", mq.ErrSendFailed, err)
	}

	return &mq.ProduceResult{
		MessageID: result.MsgID,
		Topic:     msg.Topic,
		Offset:    result.QueueOffset,
		Timestamp: time.Now(),
	}, nil
}

// SendAsync 异步发送消息
func (p *producerImpl) SendAsync(ctx context.Context, msg *mq.Message, callback func(*mq.ProduceResult, error)) error {
	if msg == nil || msg.Topic == "" {
		return mq.ErrInvalidMessage
	}

	// 创建 RocketMQ 消息
	rmqMsg := primitive.NewMessage(msg.Topic, msg.Value)
	if msg.Key != "" {
		rmqMsg.WithKeys([]string{msg.Key})
	}

	// 设置消息属性
	if len(msg.Headers) > 0 {
		for k, v := range msg.Headers {
			rmqMsg.WithProperty(k, v)
		}
	}

	// 异步发送
	err := p.producer.SendAsync(ctx, func(ctx context.Context, result *primitive.SendResult, err error) {
		if callback != nil {
			if err != nil {
				callback(nil, err)
			} else {
				callback(&mq.ProduceResult{
					MessageID: result.MsgID,
					Topic:     msg.Topic,
					Offset:    result.QueueOffset,
					Timestamp: time.Now(),
				}, nil)
			}
		}
	}, rmqMsg)

	if err != nil {
		return fmt.Errorf("%w: %v", mq.ErrSendFailed, err)
	}

	return nil
}

// SendBatch 批量发送消息
func (p *producerImpl) SendBatch(ctx context.Context, msgs []*mq.Message) ([]*mq.ProduceResult, error) {
	if len(msgs) == 0 {
		return []*mq.ProduceResult{}, nil
	}

	results := make([]*mq.ProduceResult, 0, len(msgs))
	for _, msg := range msgs {
		result, err := p.Send(ctx, msg)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}

	return results, nil
}

// Flush 刷新缓冲区
func (p *producerImpl) Flush(ctx context.Context) error {
	// RocketMQ producer 会自动刷新
	return nil
}

// GetConfig 获取配置
func (p *producerImpl) GetConfig() *mq.Config {
	configCopy := *p.config
	return &configCopy
}

// SetConfig 更新配置
func (p *producerImpl) SetConfig(config *mq.Config) {
	if config != nil {
		p.config = config
	}
}

// Close 关闭生产者
func (p *producerImpl) Close() error {
	return p.producer.Shutdown()
}

// 编译时检查
var _ mq.Producer = (*producerImpl)(nil)

