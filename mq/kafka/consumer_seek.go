package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/roc/roc-foundation-util-go/mq"
)

// consumerSeekImpl 支持 Seek 操作的 Kafka 消费者
// 使用 PartitionConsumer 而不是 ConsumerGroup
type consumerSeekImpl struct {
	client    sarama.Client
	consumers map[string]map[int32]sarama.PartitionConsumer // topic -> partition -> consumer
	config    *mq.ConsumerConfig
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
}

// NewKafkaSeekConsumer 创建支持 Seek 的 Kafka 消费者
//
// 注意：此消费者不使用消费者组，支持精确的 Seek 操作
// 适用于需要从特定 offset 开始消费的场景
func NewKafkaSeekConsumer(brokers []string, topics []string, partitions map[string][]int32) (mq.Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	impl := &consumerSeekImpl{
		client:    client,
		consumers: make(map[string]map[int32]sarama.PartitionConsumer),
		config:    mq.DefaultConsumerConfig(),
	}

	// 为每个主题的每个分区创建消费者
	for topic, parts := range partitions {
		impl.consumers[topic] = make(map[int32]sarama.PartitionConsumer)
		for _, partition := range parts {
			pc, err := sarama.NewConsumerFromClient(client)
			if err != nil {
				impl.Close()
				return nil, fmt.Errorf("failed to create consumer: %w", err)
			}

			// 默认从最新开始
			partConsumer, err := pc.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				impl.Close()
				return nil, fmt.Errorf("failed to consume partition: %w", err)
			}

			impl.consumers[topic][partition] = partConsumer
		}
	}

	return impl, nil
}

// Seek 定位到指定偏移量
func (c *consumerSeekImpl) Seek(topic string, partition int32, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	topicConsumers, exists := c.consumers[topic]
	if !exists {
		return fmt.Errorf("topic %s not subscribed", topic)
	}

	pc, exists := topicConsumers[partition]
	if !exists {
		return fmt.Errorf("partition %d not subscribed for topic %s", partition, topic)
	}

	// 关闭旧的消费者
	pc.Close()

	// 创建新的消费者，从指定 offset 开始
	consumer, err := sarama.NewConsumerFromClient(c.client)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	newPc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	// 替换消费者
	c.consumers[topic][partition] = newPc

	return nil
}

// Subscribe 订阅主题并消费消息
func (c *consumerSeekImpl) Subscribe(ctx context.Context, handler mq.ConsumeHandler) error {
	consumerCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// 为每个分区启动消费 goroutine
	for topic, partConsumers := range c.consumers {
		for partition, pc := range partConsumers {
			c.wg.Add(1)
			go func(t string, p int32, partConsumer sarama.PartitionConsumer) {
				defer c.wg.Done()

				for {
					select {
					case msg := <-partConsumer.Messages():
						if msg == nil {
							return
						}

						// 转换为 MQ 消息
						mqMsg := &mq.Message{
							Topic:     msg.Topic,
							Key:       string(msg.Key),
							Value:     msg.Value,
							Timestamp: msg.Timestamp,
							Partition: msg.Partition,
							Offset:    msg.Offset,
							Headers:   make(map[string]string),
						}

						// 转换消息头
						for _, header := range msg.Headers {
							if header != nil {
								mqMsg.Headers[string(header.Key)] = string(header.Value)
							}
						}

						// 调用处理函数
						if err := handler(mqMsg); err != nil {
							if c.config.ErrorHandler != nil {
								c.config.ErrorHandler(err)
							}
						}

					case err := <-partConsumer.Errors():
						if c.config.ErrorHandler != nil {
							c.config.ErrorHandler(err)
						}

					case <-consumerCtx.Done():
						return
					}
				}
			}(topic, partition, pc)
		}
	}

	// 等待上下文取消
	<-consumerCtx.Done()
	return nil
}

// Consume 手动消费单条消息
func (c *consumerSeekImpl) Consume(ctx context.Context) (*mq.Message, error) {
	// 从第一个可用的分区消费
	for _, partConsumers := range c.consumers {
		for _, pc := range partConsumers {
			select {
			case msg := <-pc.Messages():
				if msg != nil {
					return &mq.Message{
						Topic:     msg.Topic,
						Key:       string(msg.Key),
						Value:     msg.Value,
						Timestamp: msg.Timestamp,
						Partition: msg.Partition,
						Offset:    msg.Offset,
					}, nil
				}
			case <-ctx.Done():
				return nil, mq.ErrTimeout
			}
		}
	}
	return nil, mq.ErrConsumeFailed
}

// Commit 提交偏移量（手动消费模式不需要）
func (c *consumerSeekImpl) Commit(ctx context.Context, msg *mq.Message) error {
	return nil
}

// CommitAll 提交所有偏移量
func (c *consumerSeekImpl) CommitAll(ctx context.Context) error {
	return nil
}

// Pause 暂停消费
func (c *consumerSeekImpl) Pause(topics []string) error {
	return fmt.Errorf("seek consumer does not support pause")
}

// Resume 恢复消费
func (c *consumerSeekImpl) Resume(topics []string) error {
	return fmt.Errorf("seek consumer does not support resume")
}

// GetConfig 获取配置
func (c *consumerSeekImpl) GetConfig() *mq.ConsumerConfig {
	configCopy := *c.config
	return &configCopy
}

// SetConfig 更新配置
func (c *consumerSeekImpl) SetConfig(config *mq.ConsumerConfig) {
	if config != nil {
		c.config = config
	}
}

// Close 关闭消费者
func (c *consumerSeekImpl) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, partConsumers := range c.consumers {
		for _, pc := range partConsumers {
			pc.Close()
		}
	}

	return c.client.Close()
}

// 编译时检查
var _ mq.Consumer = (*consumerSeekImpl)(nil)
