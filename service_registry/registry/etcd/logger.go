package etcd

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetQuietLogger 返回一个静默的 logger（只记录 Error 级别）
// 用于测试环境，过滤 "context canceled" 等正常的 warn 日志
func GetQuietLogger() *zap.Logger {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	logger, err := config.Build()
	if err != nil {
		// 如果创建失败，返回 nop logger
		return zap.NewNop()
	}

	return logger
}

// GetLogger 返回配置好的 logger
func GetLogger(level zapcore.Level) *zap.Logger {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(level)

	logger, err := config.Build()
	if err != nil {
		return zap.NewNop()
	}

	return logger
}

// WithQuietLogger 返回一个带静默 logger 的 Option
// 使用示例：
//
//	reg, _ := NewEtcdRegistry(
//	    WithEndpoints(...),
//	    WithQuietLogger(),  // 过滤 warn 日志
//	)
func WithQuietLogger() Option {
	return func(c *Config) {
		// 这个需要在创建 etcd 客户端时使用
		// 我们需要修改 NewEtcdRegistry 来支持这个
	}
}
