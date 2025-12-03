package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/roc/roc-foundation-util-go/mq"
)

// producerImpl Kafka 生产者的内部实现
type producerImpl struct {
	producer sarama.SyncProducer
	config   *mq.Config
}

// NewKafkaProducer 创建 Kafka 生产者
//
// 参数：
//   - options: Kafka 生产者配置选项
//   - mqOptions: MQ 通用配置选项
//
// 返回：
//   - mq.Producer 接口实例
func NewKafkaProducer(options []ProducerOption, mqOptions ...mq.Option) (mq.Producer, error) {
	// Kafka 配置
	kafkaConfig := DefaultProducerConfig()
	for _, opt := range options {
		opt(kafkaConfig)
	}

	// MQ 通用配置
	mqConfig := mq.DefaultConfig()
	for _, opt := range mqOptions {
		opt(mqConfig)
	}
	mqConfig.Brokers = kafkaConfig.Brokers

	// 创建 Sarama 配置
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.RequiredAcks(kafkaConfig.Acks)
	config.Producer.Retry.Max = kafkaConfig.MaxRetry
	config.Producer.Timeout = kafkaConfig.Timeout
	config.Producer.Idempotent = kafkaConfig.EnableIdempotence

	// 设置压缩
	switch kafkaConfig.Compression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	// 创建生产者
	producer, err := sarama.NewSyncProducer(kafkaConfig.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &producerImpl{
		producer: producer,
		config:   mqConfig,
	}, nil
}

// Send 同步发送消息
func (p *producerImpl) Send(ctx context.Context, msg *mq.Message) (*mq.ProduceResult, error) {
	if msg == nil || msg.Topic == "" {
		return nil, mq.ErrInvalidMessage
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Key:       sarama.StringEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
		Timestamp: msg.Timestamp,
	}

	// 设置消息头
	if len(msg.Headers) > 0 {
		headers := make([]sarama.RecordHeader, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			headers = append(headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
		kafkaMsg.Headers = headers
	}

	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", mq.ErrSendFailed, err)
	}

	return &mq.ProduceResult{
		MessageID: fmt.Sprintf("%s-%d-%d", msg.Topic, partition, offset),
		Topic:     msg.Topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}, nil
}

// SendAsync 异步发送消息
func (p *producerImpl) SendAsync(ctx context.Context, msg *mq.Message, callback func(*mq.ProduceResult, error)) error {
	// Kafka 的 SyncProducer 不支持异步，使用 goroutine 模拟
	go func() {
		result, err := p.Send(ctx, msg)
		if callback != nil {
			callback(result, err)
		}
	}()
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
	// SyncProducer 是同步的，不需要刷新
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
	return p.producer.Close()
}

// 编译时检查
var _ mq.Producer = (*producerImpl)(nil)

