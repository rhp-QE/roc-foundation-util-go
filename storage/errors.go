package storage

import "errors"

var (
	// ErrNotFound 文档不存在
	ErrNotFound = errors.New("storage: document not found")

	// ErrDuplicateKey 重复键
	ErrDuplicateKey = errors.New("storage: duplicate key")

	// ErrInvalidDocument 无效的文档
	ErrInvalidDocument = errors.New("storage: invalid document")

	// ErrConnectionFailed 连接失败
	ErrConnectionFailed = errors.New("storage: connection failed")

	// ErrTimeout 操作超时
	ErrTimeout = errors.New("storage: operation timeout")

	// ErrInvalidOperation 无效的操作
	ErrInvalidOperation = errors.New("storage: invalid operation")
)

