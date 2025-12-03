package rocketmq

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/roc/roc-foundation-util-go/mq"
)

// consumerImpl RocketMQ 消费者的内部实现
type consumerImpl struct {
	consumer rocketmq.PushConsumer
	config   *mq.ConsumerConfig
	handler  mq.ConsumeHandler
}

// NewRocketMQConsumer 创建 RocketMQ 消费者
//
// 参数：
//   - options: RocketMQ 消费者配置选项
//
// 返回：
//   - mq.Consumer 接口实例
func NewRocketMQConsumer(options []ConsumerOption) (mq.Consumer, error) {
	// RocketMQ 配置
	rmqConfig := DefaultConsumerConfig()
	for _, opt := range options {
		opt(rmqConfig)
	}

	if rmqConfig.GroupName == "" {
		return nil, fmt.Errorf("group name is required")
	}

	// 创建消费者选项
	opts := []consumer.Option{
		consumer.WithNameServer(rmqConfig.NameServer),
		consumer.WithGroupName(rmqConfig.GroupName),
		consumer.WithMaxReconsumeTimes(rmqConfig.MaxReconsumeTimes),
		consumer.WithConsumeTimeout(rmqConfig.ConsumeTimeout),
	}

	// 设置消息模式
	if rmqConfig.MessageModel == Broadcasting {
		opts = append(opts, consumer.WithConsumerModel(consumer.BroadCasting))
	} else {
		opts = append(opts, consumer.WithConsumerModel(consumer.Clustering))
	}

	// 设置消费位置
	if rmqConfig.ConsumeFromWhere == mq.OffsetOldest {
		opts = append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
	} else {
		opts = append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
	}

	// 创建消费者
	c, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create rocketmq consumer: %w", err)
	}

	// 转换为 MQ 通用配置
	mqConfig := mq.DefaultConsumerConfig()
	mqConfig.GroupID = rmqConfig.GroupName
	mqConfig.Topics = rmqConfig.Topics

	impl := &consumerImpl{
		consumer: c,
		config:   mqConfig,
	}

	return impl, nil
}

// Subscribe 订阅主题并消费消息
func (c *consumerImpl) Subscribe(ctx context.Context, handler mq.ConsumeHandler) error {
	if len(c.config.Topics) == 0 {
		return mq.ErrNoTopics
	}

	c.handler = handler

	// 订阅所有主题
	for _, topic := range c.config.Topics {
		err := c.consumer.Subscribe(topic, consumer.MessageSelector{},
			func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				for _, rmqMsg := range msgs {
					// 转换为 MQ 消息
					msg := &mq.Message{
						Topic:     rmqMsg.Topic,
						Key:       rmqMsg.GetKeys(),
						Value:     rmqMsg.Body,
						Timestamp: time.UnixMilli(rmqMsg.BornTimestamp),
						Offset:    rmqMsg.QueueOffset,
						Headers:   make(map[string]string),
					}

					// 转换消息属性
					for k, v := range rmqMsg.GetProperties() {
						msg.Headers[k] = v
					}

					// 调用处理函数
					if err := handler(msg); err != nil {
						// 处理失败，返回重新消费
						if c.config.ErrorHandler != nil {
							c.config.ErrorHandler(err)
						}
						return consumer.ConsumeRetryLater, err
					}
				}

				return consumer.ConsumeSuccess, nil
			})

		if err != nil {
			return fmt.Errorf("failed to subscribe topic %s: %w", topic, err)
		}
	}

	// 启动消费者
	if err := c.consumer.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	// 阻塞直到上下文取消
	<-ctx.Done()
	return nil
}

// Consume 手动消费单条消息（RocketMQ 使用 Push 模式，不支持）
func (c *consumerImpl) Consume(ctx context.Context) (*mq.Message, error) {
	return nil, fmt.Errorf("rocketmq push consumer does not support manual consume")
}

// Commit 提交偏移量
func (c *consumerImpl) Commit(ctx context.Context, msg *mq.Message) error {
	// RocketMQ Push Consumer 自动管理偏移量
	return nil
}

// CommitAll 提交所有偏移量
func (c *consumerImpl) CommitAll(ctx context.Context) error {
	// RocketMQ Push Consumer 自动管理偏移量
	return nil
}

// Seek 定位到指定偏移量
func (c *consumerImpl) Seek(topic string, partition int32, offset int64) error {
	// RocketMQ 不直接支持 Seek
	return fmt.Errorf("rocketmq consumer does not support seek")
}

// Pause 暂停消费
func (c *consumerImpl) Pause(topics []string) error {
	// RocketMQ 暂停
	c.consumer.Suspend()
	return nil
}

// Resume 恢复消费
func (c *consumerImpl) Resume(topics []string) error {
	// RocketMQ 恢复
	c.consumer.Resume()
	return nil
}

// GetConfig 获取配置
func (c *consumerImpl) GetConfig() *mq.ConsumerConfig {
	configCopy := *c.config
	return &configCopy
}

// SetConfig 更新配置
func (c *consumerImpl) SetConfig(config *mq.ConsumerConfig) {
	if config != nil {
		c.config = config
	}
}

// Close 关闭消费者
func (c *consumerImpl) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Shutdown()
}

// 编译时检查
var _ mq.Consumer = (*consumerImpl)(nil)
