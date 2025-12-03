package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/roc/roc-foundation-util-go/mq"
)

// consumerImpl Kafka 消费者的内部实现
type consumerImpl struct {
	consumer sarama.ConsumerGroup
	config   *mq.ConsumerConfig
	topics   []string
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewKafkaConsumer 创建 Kafka 消费者
//
// 参数：
//   - options: Kafka 消费者配置选项
//
// 返回：
//   - mq.Consumer 接口实例
func NewKafkaConsumer(options []ConsumerOption) (mq.Consumer, error) {
	// Kafka 配置
	kafkaConfig := DefaultConsumerConfig()
	for _, opt := range options {
		opt(kafkaConfig)
	}

	if kafkaConfig.GroupID == "" {
		return nil, fmt.Errorf("group id is required")
	}

	// 创建 Sarama 配置
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	if kafkaConfig.InitialOffset == mq.OffsetOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Consumer.Offsets.AutoCommit.Enable = kafkaConfig.AutoCommit
	config.Consumer.Offsets.AutoCommit.Interval = kafkaConfig.AutoCommitInterval
	config.Consumer.Group.Session.Timeout = kafkaConfig.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = kafkaConfig.HeartbeatInterval

	// 创建消费者组
	consumer, err := sarama.NewConsumerGroup(kafkaConfig.Brokers, kafkaConfig.GroupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	// 转换为 MQ 通用配置
	mqConfig := mq.DefaultConsumerConfig()
	mqConfig.GroupID = kafkaConfig.GroupID
	mqConfig.Topics = kafkaConfig.Topics
	mqConfig.AutoCommit = kafkaConfig.AutoCommit
	mqConfig.AutoCommitInterval = kafkaConfig.AutoCommitInterval
	mqConfig.InitialOffset = kafkaConfig.InitialOffset

	return &consumerImpl{
		consumer: consumer,
		config:   mqConfig,
		topics:   kafkaConfig.Topics,
	}, nil
}

// Subscribe 订阅主题并消费消息
func (c *consumerImpl) Subscribe(ctx context.Context, handler mq.ConsumeHandler) error {
	if len(c.topics) == 0 {
		return mq.ErrNoTopics
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	consumerHandler := &consumerGroupHandler{
		handler: handler,
		ready:   make(chan bool),
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.consumer.Consume(consumerCtx, c.topics, consumerHandler); err != nil {
				if c.config.ErrorHandler != nil {
					c.config.ErrorHandler(err)
				}
			}

			// 检查上下文是否被取消
			if consumerCtx.Err() != nil {
				return
			}

			consumerHandler.ready = make(chan bool)
		}
	}()

	<-consumerHandler.ready // 等待消费者准备就绪
	return nil
}

// Consume 手动消费单条消息（不支持，Kafka 使用订阅模式）
func (c *consumerImpl) Consume(ctx context.Context) (*mq.Message, error) {
	return nil, fmt.Errorf("kafka consumer does not support manual consume, use Subscribe instead")
}

// Commit 提交偏移量
func (c *consumerImpl) Commit(ctx context.Context, msg *mq.Message) error {
	// Kafka ConsumerGroup 自动管理偏移量
	return nil
}

// CommitAll 提交所有偏移量
func (c *consumerImpl) CommitAll(ctx context.Context) error {
	// Kafka ConsumerGroup 自动管理偏移量
	return nil
}

// Seek 定位到指定偏移量
func (c *consumerImpl) Seek(topic string, partition int32, offset int64) error {
	// Kafka ConsumerGroup 不直接支持 Seek，需要在消费时处理
	return fmt.Errorf("kafka consumer group does not support seek")
}

// Pause 暂停消费
func (c *consumerImpl) Pause(topics []string) error {
	return fmt.Errorf("kafka consumer group pause not implemented")
}

// Resume 恢复消费
func (c *consumerImpl) Resume(topics []string) error {
	return fmt.Errorf("kafka consumer group resume not implemented")
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
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return c.consumer.Close()
}

// consumerGroupHandler 消费者组处理器
type consumerGroupHandler struct {
	handler mq.ConsumeHandler
	ready   chan bool
}

// Setup 在消费开始前调用
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

// Cleanup 在消费结束后调用
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理消息
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// 转换为 MQ 消息
			msg := &mq.Message{
				Topic:     message.Topic,
				Key:       string(message.Key),
				Value:     message.Value,
				Timestamp: message.Timestamp,
				Partition: message.Partition,
				Offset:    message.Offset,
				Headers:   make(map[string]string),
			}

			// 转换消息头
			for _, header := range message.Headers {
				msg.Headers[string(header.Key)] = string(header.Value)
			}

			// 调用处理函数
			if err := h.handler(msg); err != nil {
				// 处理失败，可以选择跳过或重试
				continue
			}

			// 标记消息已处理
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// 编译时检查
var _ mq.Consumer = (*consumerImpl)(nil)

