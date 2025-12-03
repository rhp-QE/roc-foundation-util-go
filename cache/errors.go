package cache

import "errors"

var (
	// ErrKeyNotFound 键不存在
	ErrKeyNotFound = errors.New("cache: key not found")

	// ErrNilValue 值为空
	ErrNilValue = errors.New("cache: nil value")

	// ErrInvalidValue 无效的值
	ErrInvalidValue = errors.New("cache: invalid value")

	// ErrConnectionFailed 连接失败
	ErrConnectionFailed = errors.New("cache: connection failed")

	// ErrTimeout 操作超时
	ErrTimeout = errors.New("cache: operation timeout")
)

