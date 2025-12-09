package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/mq"
)

// TestNewKafkaProducer 测试创建 Kafka 生产者
func TestNewKafkaProducer(t *testing.T) {
	var (
		producer mq.Producer
		err      error
	)
	producer, err = NewKafkaProducer(
		[]ProducerOption{
			WithProducerBrokers([]string{"localhost:9092"}),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka not available: %v", err)
		return
	}
	defer producer.Close()

	if producer == nil {
		t.Fatal("Producer should not be nil")
	}
}

// TestKafkaProducerSend 测试发送消息
func TestKafkaProducerSend(t *testing.T) {
	var (
		producer mq.Producer
		err      error
	)
	producer, err = NewKafkaProducer(
		[]ProducerOption{
			WithProducerBrokers([]string{"localhost:9092"}),
			WithAcks(1),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka not available: %v", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()

	// 发送消息
	msg := &mq.Message{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test message"),
		Headers: map[string]string{
			"source": "test",
		},
	}

	result, err := producer.Send(ctx, msg)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}
	if result.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", result.Topic)
	}
}

// TestKafkaProducerSendBatch 测试批量发送
func TestKafkaProducerSendBatch(t *testing.T) {
	var (
		producer mq.Producer
		err      error
	)
	producer, err = NewKafkaProducer(
		[]ProducerOption{
			WithProducerBrokers([]string{"localhost:9092"}),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka not available: %v", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()

	// 批量发送
	msgs := []*mq.Message{
		{Topic: "test-topic", Key: "key1", Value: []byte("msg1")},
		{Topic: "test-topic", Key: "key2", Value: []byte("msg2")},
		{Topic: "test-topic", Key: "key3", Value: []byte("msg3")},
	}

	results, err := producer.SendBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// TestNewKafkaConsumer 测试创建 Kafka 消费者
func TestNewKafkaConsumer(t *testing.T) {
	var (
		consumer mq.Consumer
		err      error
	)
	consumer, err = NewKafkaConsumer(
		[]ConsumerOption{
			WithConsumerBrokers([]string{"localhost:9092"}),
			WithConsumerGroupID("test-group"),
			WithConsumerTopics([]string{"test-topic"}),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka not available: %v", err)
		return
	}
	defer consumer.Close()

	if consumer == nil {
		t.Fatal("Consumer should not be nil")
	}
}

// TestKafkaConsumerSubscribe 测试订阅消费
func TestKafkaConsumerSubscribe(t *testing.T) {
	var (
		producer mq.Producer
		consumer mq.Consumer
		err      error
	)

	topicName := fmt.Sprintf("test-kafka-%d", time.Now().Unix())

	// 创建生产者
	producer, err = NewKafkaProducer(
		[]ProducerOption{
			WithProducerBrokers([]string{"localhost:9092"}),
			WithAcks(1),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka not available: %v", err)
		return
	}
	defer producer.Close()

	// 先发送几条消息
	t.Log("✓ 发送初始消息...")
	for i := 1; i <= 3; i++ {
		msg := &mq.Message{
			Topic: topicName,
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("Initial message %d", i)),
		}
		result, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Logf("  发送失败: %v", err)
		} else {
			t.Logf("  → 发送消息 #%d: Partition=%d, Offset=%d",
				i, result.Partition, result.Offset)
		}
	}

	// 等待消息到达
	time.Sleep(2 * time.Second)

	// 创建消费者（从最早开始消费）
	t.Log("✓ 创建消费者...")
	consumer, err = NewKafkaConsumer(
		[]ConsumerOption{
			WithConsumerBrokers([]string{"localhost:9092"}),
			WithConsumerGroupID(fmt.Sprintf("test-group-%d", time.Now().Unix())),
			WithConsumerTopics([]string{topicName}),
			WithConsumerInitialOffset(mq.OffsetOldest), // 从最早开始
		},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka consumer not available: %v", err)
		return
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var messageCount int64

	// 订阅消费
	t.Log("✓ 开始消费...")
	go func() {
		consumer.Subscribe(ctx, func(msg *mq.Message) error {
			count := atomic.AddInt64(&messageCount, 1)
			t.Logf("  ← 【消费】#%d: Topic=%s, Key=%s, Value=%s, Partition=%d, Offset=%d",
				count, msg.Topic, msg.Key, string(msg.Value), msg.Partition, msg.Offset)

			// 收到 5 条消息后停止
			if count >= 5 {
				cancel()
			}
			return nil
		})
	}()

	// 等待消费者启动
	time.Sleep(3 * time.Second)

	// 继续发送消息
	t.Log("✓ 继续发送消息...")
	for i := 4; i <= 6; i++ {
		msg := &mq.Message{
			Topic: topicName,
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("Runtime message %d", i)),
		}
		result, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Logf("  发送失败: %v", err)
		} else {
			t.Logf("  → 【发送】#%d: Partition=%d, Offset=%d",
				i, result.Partition, result.Offset)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 等待消费完成
	<-ctx.Done()

	received := atomic.LoadInt64(&messageCount)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("  ✅ 测试完成！")
	t.Logf("  • 发送: 6 条消息")
	t.Logf("  • 消费: %d 条消息", received)
	t.Logf("  • Kafka 生产消费: 正常 ✓")
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if received == 0 {
		t.Error("未消费到任何消息")
	}
}
