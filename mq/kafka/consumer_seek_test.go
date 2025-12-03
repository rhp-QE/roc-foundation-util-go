package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roc/roc-foundation-util-go/mq"
)

// TestKafkaSeekConsumer 测试支持 Seek 的消费者
func TestKafkaSeekConsumer(t *testing.T) {
	var (
		producer mq.Producer
		consumer mq.Consumer
		err      error
	)

	topicName := fmt.Sprintf("test-seek-%d", time.Now().Unix())

	// 1. 创建生产者
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

	// 2. 发送 10 条消息
	t.Log("✓ 发送 10 条消息...")
	for i := 1; i <= 10; i++ {
		msg := &mq.Message{
			Topic: topicName,
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("Message %d", i)),
		}
		result, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Fatalf("发送消息失败: %v", err)
		}
		t.Logf("  → 消息 #%d: Partition=%d, Offset=%d",
			i, result.Partition, result.Offset)
	}

	// 等待消息写入
	time.Sleep(2 * time.Second)

	// 3. 创建 Seek 消费者（只消费分区 0）
	t.Log("✓ 创建支持 Seek 的消费者...")
	consumer, err = NewKafkaSeekConsumer(
		[]string{"localhost:9092"},
		[]string{topicName},
		map[string][]int32{topicName: {0}}, // 只消费分区 0
	)
	if err != nil {
		t.Skipf("Skipping test, kafka seek consumer not available: %v", err)
		return
	}
	defer consumer.Close()

	// 4. 从 offset 0 开始消费
	t.Log("✓ Seek 到 offset 0...")
	err = consumer.Seek(topicName, 0, 0)
	if err != nil {
		t.Fatalf("Seek 失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var messageCount int64
	var firstOffset int64 = -1
	var lastOffset int64 = -1

	// 5. 开始消费
	t.Log("✓ 开始从 offset 0 消费...")
	go func() {
		consumer.Subscribe(ctx, func(msg *mq.Message) error {
			count := atomic.AddInt64(&messageCount, 1)

			if firstOffset == -1 {
				firstOffset = msg.Offset
			}
			lastOffset = msg.Offset

			t.Logf("  ← 消费 #%d: Offset=%d, Key=%s, Value=%s",
				count, msg.Offset, msg.Key, string(msg.Value))

			// 收到 5 条后停止
			if count >= 5 {
				cancel()
			}
			return nil
		})
	}()

	// 等待消费完成
	<-ctx.Done()
	time.Sleep(500 * time.Millisecond)

	received := atomic.LoadInt64(&messageCount)

	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("  第一阶段测试结果:")
	t.Logf("  • 消费消息数: %d", received)
	t.Logf("  • 起始 Offset: %d", firstOffset)
	t.Logf("  • 结束 Offset: %d", lastOffset)
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if firstOffset != 0 {
		t.Errorf("期望从 offset 0 开始，实际从 %d 开始", firstOffset)
	}

	// 6. 测试 Seek 到中间位置
	t.Log("")
	t.Log("✓ 测试 Seek 到 offset 5...")

	err = consumer.Seek(topicName, 0, 5)
	if err != nil {
		t.Fatalf("Seek 到 offset 5 失败: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	atomic.StoreInt64(&messageCount, 0)
	firstOffset = -1
	lastOffset = -1

	t.Log("✓ 开始从 offset 5 消费...")
	go func() {
		consumer.Subscribe(ctx2, func(msg *mq.Message) error {
			count := atomic.AddInt64(&messageCount, 1)

			if firstOffset == -1 {
				firstOffset = msg.Offset
			}
			lastOffset = msg.Offset

			t.Logf("  ← 消费 #%d: Offset=%d, Value=%s",
				count, msg.Offset, string(msg.Value))

			// 收到 3 条后停止
			if count >= 3 {
				cancel2()
			}
			return nil
		})
	}()

	<-ctx2.Done()
	time.Sleep(500 * time.Millisecond)

	received2 := atomic.LoadInt64(&messageCount)

	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("  第二阶段测试结果:")
	t.Logf("  • 消费消息数: %d", received2)
	t.Logf("  • 起始 Offset: %d", firstOffset)
	t.Logf("  • 结束 Offset: %d", lastOffset)
	t.Log("")
	t.Log("  ✅ Seek 功能验证:")
	if firstOffset == 5 {
		t.Log("  ✓ 成功从 offset 5 开始消费")
	} else {
		t.Errorf("  ✗ 期望从 offset 5 开始，实际从 %d 开始", firstOffset)
	}
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

// TestKafkaSeekToSpecificMessage 测试 Seek 到特定消息
func TestKafkaSeekToSpecificMessage(t *testing.T) {
	var (
		producer mq.Producer
		consumer mq.Consumer
		err      error
	)

	topicName := fmt.Sprintf("test-seek-specific-%d", time.Now().Unix())

	// 创建生产者
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

	// 发送消息并记录 offset
	t.Log("✓ 发送带时间戳的消息...")
	var targetOffset int64
	for i := 1; i <= 20; i++ {
		msg := &mq.Message{
			Topic: topicName,
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("Timestamped message %d at %s",
				i, time.Now().Format("15:04:05.000"))),
		}
		result, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Fatalf("发送失败: %v", err)
		}

		// 记录第 10 条消息的 offset
		if i == 10 {
			targetOffset = result.Offset
			t.Logf("  ★ 目标消息 #10: Offset=%d", targetOffset)
		}

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)

	// 创建 Seek 消费者
	consumer, err = NewKafkaSeekConsumer(
		[]string{"localhost:9092"},
		[]string{topicName},
		map[string][]int32{topicName: {0}},
	)
	if err != nil {
		t.Skipf("Skipping test, kafka seek consumer not available: %v", err)
		return
	}
	defer consumer.Close()

	// Seek 到第 10 条消息
	t.Logf("✓ Seek 到 offset %d（第 10 条消息）...", targetOffset)
	err = consumer.Seek(topicName, 0, targetOffset)
	if err != nil {
		t.Fatalf("Seek 失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		messageCount int64
		firstMsg     *mq.Message
	)

	// 开始消费
	t.Log("✓ 验证是否从第 10 条消息开始...")
	go func() {
		consumer.Subscribe(ctx, func(msg *mq.Message) error {
			count := atomic.AddInt64(&messageCount, 1)

			if count == 1 {
				firstMsg = msg
			}

			t.Logf("  ← 消费 #%d: Offset=%d, Value=%s",
				count, msg.Offset, string(msg.Value))

			if count >= 3 {
				cancel()
			}
			return nil
		})
	}()

	<-ctx.Done()
	time.Sleep(500 * time.Millisecond)

	// 验证结果
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Log("  Seek 精确定位测试结果:")
	if firstMsg != nil {
		t.Logf("  • 第一条消费的消息 Offset: %d", firstMsg.Offset)
		t.Logf("  • 目标 Offset: %d", targetOffset)

		if firstMsg.Offset == targetOffset {
			t.Log("  ✅ Seek 成功！精确定位到指定消息")
		} else {
			t.Errorf("  ✗ Seek 失败：期望 %d，实际 %d",
				targetOffset, firstMsg.Offset)
		}
	} else {
		t.Error("  ✗ 未消费到任何消息")
	}
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

// TestKafkaSeekMultiplePartitions 测试多分区 Seek
func TestKafkaSeekMultiplePartitions(t *testing.T) {
	// 注意：这个测试需要手动创建多分区的 Topic
	// 可以使用 kafka-topics 命令创建
	t.Skip("需要手动创建多分区 Topic，跳过此测试")

	// 示例代码（取消注释后可用）：
	/*
		consumer, _ := NewKafkaSeekConsumer(
			[]string{"localhost:9092"},
			[]string{"multi-partition-topic"},
			map[string][]int32{
				"multi-partition-topic": {0, 1, 2},  // 3个分区
			},
		)
		defer consumer.Close()

		// Seek 不同分区到不同位置
		consumer.Seek("multi-partition-topic", 0, 100)
		consumer.Seek("multi-partition-topic", 1, 200)
		consumer.Seek("multi-partition-topic", 2, 300)

		// 开始消费
		consumer.Subscribe(ctx, handleMessage)
	*/
}
