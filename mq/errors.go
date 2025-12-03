package mq

import "errors"

var (
	// ErrProducerClosed 生产者已关闭
	ErrProducerClosed = errors.New("mq: producer closed")

	// ErrConsumerClosed 消费者已关闭
	ErrConsumerClosed = errors.New("mq: consumer closed")

	// ErrInvalidMessage 无效的消息
	ErrInvalidMessage = errors.New("mq: invalid message")

	// ErrSendFailed 发送失败
	ErrSendFailed = errors.New("mq: send failed")

	// ErrConsumeFailed 消费失败
	ErrConsumeFailed = errors.New("mq: consume failed")

	// ErrConnectionFailed 连接失败
	ErrConnectionFailed = errors.New("mq: connection failed")

	// ErrTimeout 操作超时
	ErrTimeout = errors.New("mq: operation timeout")

	// ErrNoTopics 没有订阅主题
	ErrNoTopics = errors.New("mq: no topics subscribed")

	// ErrCommitFailed 提交失败
	ErrCommitFailed = errors.New("mq: commit failed")
)

