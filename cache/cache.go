package cache

import (
	"context"
	"time"
)

// Cache 缓存接口
//
// 定义了缓存的核心功能，让使用者快速了解所有可用方法。
//
// 使用示例：
//
//	cache := redis.NewRedisCache(
//	    redis.WithAddress("localhost:6379"),
//	    redis.WithPassword(""),
//	)
//	defer cache.Close()
//
//	// 设置缓存
//	cache.Set(ctx, "key", "value", 5*time.Minute)
//
//	// 获取缓存
//	value, err := cache.Get(ctx, "key")
type Cache interface {
	// Get 获取缓存值
	Get(ctx context.Context, key string) (string, error)

	// Set 设置缓存值
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Delete 删除缓存
	Delete(ctx context.Context, keys ...string) error

	// Exists 检查键是否存在
	Exists(ctx context.Context, keys ...string) (int64, error)

	// Expire 设置过期时间
	Expire(ctx context.Context, key string, ttl time.Duration) error

	// TTL 获取剩余过期时间
	TTL(ctx context.Context, key string) (time.Duration, error)

	// MGet 批量获取
	MGet(ctx context.Context, keys ...string) ([]interface{}, error)

	// MSet 批量设置
	MSet(ctx context.Context, pairs map[string]interface{}) error

	// Incr 自增
	Incr(ctx context.Context, key string) (int64, error)

	// Decr 自减
	Decr(ctx context.Context, key string) (int64, error)

	// IncrBy 增加指定值
	IncrBy(ctx context.Context, key string, value int64) (int64, error)

	// DecrBy 减少指定值
	DecrBy(ctx context.Context, key string, value int64) (int64, error)

	// ==================== List 操作 ====================

	// LPush 从列表左侧插入元素
	LPush(ctx context.Context, key string, values ...interface{}) (int64, error)

	// RPush 从列表右侧插入元素
	RPush(ctx context.Context, key string, values ...interface{}) (int64, error)

	// LPop 从列表左侧弹出元素
	LPop(ctx context.Context, key string) (string, error)

	// RPop 从列表右侧弹出元素
	RPop(ctx context.Context, key string) (string, error)

	// LRange 获取列表指定范围的元素
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)

	// LLen 获取列表长度
	LLen(ctx context.Context, key string) (int64, error)

	// LRem 删除列表中的元素
	LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error)

	// ==================== Set 操作 ====================

	// SAdd 向集合添加成员
	SAdd(ctx context.Context, key string, members ...interface{}) (int64, error)

	// SRem 从集合移除成员
	SRem(ctx context.Context, key string, members ...interface{}) (int64, error)

	// SMembers 获取集合所有成员
	SMembers(ctx context.Context, key string) ([]string, error)

	// SIsMember 判断元素是否是集合成员
	SIsMember(ctx context.Context, key string, member interface{}) (bool, error)

	// SCard 获取集合成员数量
	SCard(ctx context.Context, key string) (int64, error)

	// SUnion 返回多个集合的并集
	SUnion(ctx context.Context, keys ...string) ([]string, error)

	// SInter 返回多个集合的交集
	SInter(ctx context.Context, keys ...string) ([]string, error)

	// SDiff 返回多个集合的差集
	SDiff(ctx context.Context, keys ...string) ([]string, error)

	// ==================== Sorted Set 操作 ====================

	// ZAdd 向有序集合添加成员
	ZAdd(ctx context.Context, key string, members ...ZMember) (int64, error)

	// ZRem 从有序集合移除成员
	ZRem(ctx context.Context, key string, members ...interface{}) (int64, error)

	// ZRange 按索引范围获取有序集合成员（分数从低到高）
	ZRange(ctx context.Context, key string, start, stop int64) ([]string, error)

	// ZRevRange 按索引范围获取有序集合成员（分数从高到低）
	ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error)

	// ZRangeByScore 按分数范围获取有序集合成员
	ZRangeByScore(ctx context.Context, key string, min, max float64) ([]string, error)

	// ZScore 获取成员的分数
	ZScore(ctx context.Context, key string, member string) (float64, error)

	// ZCard 获取有序集合成员数量
	ZCard(ctx context.Context, key string) (int64, error)

	// ZIncrBy 增加成员的分数
	ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error)

	// ==================== Hash 操作 ====================

	// HSet 设置哈希表字段的值
	HSet(ctx context.Context, key string, field string, value interface{}) error

	// HGet 获取哈希表字段的值
	HGet(ctx context.Context, key, field string) (string, error)

	// HMSet 批量设置哈希表字段
	HMSet(ctx context.Context, key string, fields map[string]interface{}) error

	// HMGet 批量获取哈希表字段
	HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error)

	// HGetAll 获取哈希表所有字段和值
	HGetAll(ctx context.Context, key string) (map[string]string, error)

	// HDel 删除哈希表字段
	HDel(ctx context.Context, key string, fields ...string) (int64, error)

	// HExists 判断哈希表字段是否存在
	HExists(ctx context.Context, key, field string) (bool, error)

	// HLen 获取哈希表字段数量
	HLen(ctx context.Context, key string) (int64, error)

	// HKeys 获取哈希表所有字段名
	HKeys(ctx context.Context, key string) ([]string, error)

	// HVals 获取哈希表所有值
	HVals(ctx context.Context, key string) ([]string, error)

	// HIncrBy 增加哈希表字段的整数值
	HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error)

	// ==================== 通用操作 ====================

	// GetConfig 获取配置
	GetConfig() *Config

	// SetConfig 更新配置
	SetConfig(config *Config)

	// Close 关闭连接
	Close() error

	// Ping 健康检查
	Ping(ctx context.Context) error
}

// ZMember 有序集合成员
type ZMember struct {
	Score  float64
	Member interface{}
}

// Config 缓存配置
type Config struct {
	// KeyPrefix 键前缀
	KeyPrefix string

	// DefaultTTL 默认过期时间
	DefaultTTL time.Duration

	// EnableMetrics 是否启用监控
	EnableMetrics bool

	// EnableTracing 是否启用追踪
	EnableTracing bool
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		KeyPrefix:     "",
		DefaultTTL:    5 * time.Minute,
		EnableMetrics: false,
		EnableTracing: false,
	}
}

// Option 配置选项
type Option func(*Config)

// WithKeyPrefix 设置键前缀
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) {
		c.KeyPrefix = prefix
	}
}

// WithDefaultTTL 设置默认过期时间
func WithDefaultTTL(ttl time.Duration) Option {
	return func(c *Config) {
		c.DefaultTTL = ttl
	}
}

// WithMetrics 设置是否启用监控
func WithMetrics(enabled bool) Option {
	return func(c *Config) {
		c.EnableMetrics = enabled
	}
}

// WithTracing 设置是否启用追踪
func WithTracing(enabled bool) Option {
	return func(c *Config) {
		c.EnableTracing = enabled
	}
}
