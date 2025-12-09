package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rhp-QE/roc-foundation-util-go/cache"
)

// impl Redis 缓存的内部实现
//
// 实现 cache.Cache 接口，对外不暴露。
type impl struct {
	client *redis.Client
	config *cache.Config
}

// NewRedisCache 创建 Redis 缓存实例
//
// 参数：
//   - options: Redis 配置选项
//   - cacheOptions: 缓存通用配置选项
//
// 返回：
//   - cache.Cache 接口实例
func NewRedisCache(options []Option, cacheOptions ...cache.Option) (cache.Cache, error) {
	// Redis 配置
	redisConfig := DefaultConfig()
	for _, opt := range options {
		opt(redisConfig)
	}

	// 缓存通用配置
	cacheConfig := cache.DefaultConfig()
	for _, opt := range cacheOptions {
		opt(cacheConfig)
	}

	// 创建 Redis 客户端
	client := redis.NewClient(redisConfig.ToRedisOptions())

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &impl{
		client: client,
		config: cacheConfig,
	}, nil
}

// buildKey 构建完整的键名
func (i *impl) buildKey(key string) string {
	if i.config.KeyPrefix == "" {
		return key
	}
	return i.config.KeyPrefix + key
}

// Get 获取缓存值
func (i *impl) Get(ctx context.Context, key string) (string, error) {
	result, err := i.client.Get(ctx, i.buildKey(key)).Result()
	if err == redis.Nil {
		return "", cache.ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("redis get error: %w", err)
	}
	return result, nil
}

// Set 设置缓存值
func (i *impl) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 如果 ttl 为 0，使用默认 TTL
	if ttl == 0 {
		ttl = i.config.DefaultTTL
	}

	// 序列化值
	var data string
	switch v := value.(type) {
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
		data = string(bytes)
	}

	err := i.client.Set(ctx, i.buildKey(key), data, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}
	return nil
}

// Delete 删除缓存
func (i *impl) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}

	err := i.client.Del(ctx, fullKeys...).Err()
	if err != nil {
		return fmt.Errorf("redis delete error: %w", err)
	}
	return nil
}

// Exists 检查键是否存在
func (i *impl) Exists(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}

	count, err := i.client.Exists(ctx, fullKeys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis exists error: %w", err)
	}
	return count, nil
}

// Expire 设置过期时间
func (i *impl) Expire(ctx context.Context, key string, ttl time.Duration) error {
	err := i.client.Expire(ctx, i.buildKey(key), ttl).Err()
	if err != nil {
		return fmt.Errorf("redis expire error: %w", err)
	}
	return nil
}

// TTL 获取剩余过期时间
func (i *impl) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := i.client.TTL(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis ttl error: %w", err)
	}
	return ttl, nil
}

// MGet 批量获取
func (i *impl) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	if len(keys) == 0 {
		return []interface{}{}, nil
	}

	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}

	values, err := i.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget error: %w", err)
	}
	return values, nil
}

// MSet 批量设置
func (i *impl) MSet(ctx context.Context, pairs map[string]interface{}) error {
	if len(pairs) == 0 {
		return nil
	}

	// 构建参数
	args := make([]interface{}, 0, len(pairs)*2)
	for key, value := range pairs {
		fullKey := i.buildKey(key)

		// 序列化值
		var data string
		switch v := value.(type) {
		case string:
			data = v
		case []byte:
			data = string(v)
		default:
			bytes, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
			}
			data = string(bytes)
		}

		args = append(args, fullKey, data)
	}

	err := i.client.MSet(ctx, args...).Err()
	if err != nil {
		return fmt.Errorf("redis mset error: %w", err)
	}
	return nil
}

// Incr 自增
func (i *impl) Incr(ctx context.Context, key string) (int64, error) {
	value, err := i.client.Incr(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis incr error: %w", err)
	}
	return value, nil
}

// Decr 自减
func (i *impl) Decr(ctx context.Context, key string) (int64, error) {
	value, err := i.client.Decr(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis decr error: %w", err)
	}
	return value, nil
}

// IncrBy 增加指定值
func (i *impl) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	result, err := i.client.IncrBy(ctx, i.buildKey(key), value).Result()
	if err != nil {
		return 0, fmt.Errorf("redis incrby error: %w", err)
	}
	return result, nil
}

// DecrBy 减少指定值
func (i *impl) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	result, err := i.client.DecrBy(ctx, i.buildKey(key), value).Result()
	if err != nil {
		return 0, fmt.Errorf("redis decrby error: %w", err)
	}
	return result, nil
}

// GetConfig 获取配置
func (i *impl) GetConfig() *cache.Config {
	// 返回配置的副本
	configCopy := *i.config
	return &configCopy
}

// SetConfig 更新配置
func (i *impl) SetConfig(config *cache.Config) {
	if config != nil {
		i.config = config
	}
}

// Close 关闭连接
func (i *impl) Close() error {
	return i.client.Close()
}

// Ping 健康检查
func (i *impl) Ping(ctx context.Context) error {
	return i.client.Ping(ctx).Err()
}

// ==================== List 操作实现 ====================

// LPush 从列表左侧插入元素
func (i *impl) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	count, err := i.client.LPush(ctx, i.buildKey(key), values...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis lpush error: %w", err)
	}
	return count, nil
}

// RPush 从列表右侧插入元素
func (i *impl) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	count, err := i.client.RPush(ctx, i.buildKey(key), values...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis rpush error: %w", err)
	}
	return count, nil
}

// LPop 从列表左侧弹出元素
func (i *impl) LPop(ctx context.Context, key string) (string, error) {
	value, err := i.client.LPop(ctx, i.buildKey(key)).Result()
	if err == redis.Nil {
		return "", cache.ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("redis lpop error: %w", err)
	}
	return value, nil
}

// RPop 从列表右侧弹出元素
func (i *impl) RPop(ctx context.Context, key string) (string, error) {
	value, err := i.client.RPop(ctx, i.buildKey(key)).Result()
	if err == redis.Nil {
		return "", cache.ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("redis rpop error: %w", err)
	}
	return value, nil
}

// LRange 获取列表指定范围的元素
func (i *impl) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	values, err := i.client.LRange(ctx, i.buildKey(key), start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lrange error: %w", err)
	}
	return values, nil
}

// LLen 获取列表长度
func (i *impl) LLen(ctx context.Context, key string) (int64, error) {
	length, err := i.client.LLen(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis llen error: %w", err)
	}
	return length, nil
}

// LRem 删除列表中的元素
func (i *impl) LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error) {
	removed, err := i.client.LRem(ctx, i.buildKey(key), count, value).Result()
	if err != nil {
		return 0, fmt.Errorf("redis lrem error: %w", err)
	}
	return removed, nil
}

// ==================== Set 操作实现 ====================

// SAdd 向集合添加成员
func (i *impl) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	count, err := i.client.SAdd(ctx, i.buildKey(key), members...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis sadd error: %w", err)
	}
	return count, nil
}

// SRem 从集合移除成员
func (i *impl) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	count, err := i.client.SRem(ctx, i.buildKey(key), members...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis srem error: %w", err)
	}
	return count, nil
}

// SMembers 获取集合所有成员
func (i *impl) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := i.client.SMembers(ctx, i.buildKey(key)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers error: %w", err)
	}
	return members, nil
}

// SIsMember 判断元素是否是集合成员
func (i *impl) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	isMember, err := i.client.SIsMember(ctx, i.buildKey(key), member).Result()
	if err != nil {
		return false, fmt.Errorf("redis sismember error: %w", err)
	}
	return isMember, nil
}

// SCard 获取集合成员数量
func (i *impl) SCard(ctx context.Context, key string) (int64, error) {
	count, err := i.client.SCard(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis scard error: %w", err)
	}
	return count, nil
}

// SUnion 返回多个集合的并集
func (i *impl) SUnion(ctx context.Context, keys ...string) ([]string, error) {
	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}
	members, err := i.client.SUnion(ctx, fullKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis sunion error: %w", err)
	}
	return members, nil
}

// SInter 返回多个集合的交集
func (i *impl) SInter(ctx context.Context, keys ...string) ([]string, error) {
	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}
	members, err := i.client.SInter(ctx, fullKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis sinter error: %w", err)
	}
	return members, nil
}

// SDiff 返回多个集合的差集
func (i *impl) SDiff(ctx context.Context, keys ...string) ([]string, error) {
	fullKeys := make([]string, len(keys))
	for idx, key := range keys {
		fullKeys[idx] = i.buildKey(key)
	}
	members, err := i.client.SDiff(ctx, fullKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis sdiff error: %w", err)
	}
	return members, nil
}

// ==================== Sorted Set 操作实现 ====================

// ZAdd 向有序集合添加成员
func (i *impl) ZAdd(ctx context.Context, key string, members ...cache.ZMember) (int64, error) {
	zMembers := make([]redis.Z, len(members))
	for idx, m := range members {
		zMembers[idx] = redis.Z{
			Score:  m.Score,
			Member: m.Member,
		}
	}
	count, err := i.client.ZAdd(ctx, i.buildKey(key), zMembers...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis zadd error: %w", err)
	}
	return count, nil
}

// ZRem 从有序集合移除成员
func (i *impl) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	count, err := i.client.ZRem(ctx, i.buildKey(key), members...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis zrem error: %w", err)
	}
	return count, nil
}

// ZRange 按索引范围获取有序集合成员（分数从低到高）
func (i *impl) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	members, err := i.client.ZRange(ctx, i.buildKey(key), start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis zrange error: %w", err)
	}
	return members, nil
}

// ZRevRange 按索引范围获取有序集合成员（分数从高到低）
func (i *impl) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	members, err := i.client.ZRevRange(ctx, i.buildKey(key), start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("redis zrevrange error: %w", err)
	}
	return members, nil
}

// ZRangeByScore 按分数范围获取有序集合成员
func (i *impl) ZRangeByScore(ctx context.Context, key string, min, max float64) ([]string, error) {
	opt := &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", min),
		Max: fmt.Sprintf("%f", max),
	}
	members, err := i.client.ZRangeByScore(ctx, i.buildKey(key), opt).Result()
	if err != nil {
		return nil, fmt.Errorf("redis zrangebyscore error: %w", err)
	}
	return members, nil
}

// ZScore 获取成员的分数
func (i *impl) ZScore(ctx context.Context, key string, member string) (float64, error) {
	score, err := i.client.ZScore(ctx, i.buildKey(key), member).Result()
	if err == redis.Nil {
		return 0, cache.ErrKeyNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("redis zscore error: %w", err)
	}
	return score, nil
}

// ZCard 获取有序集合成员数量
func (i *impl) ZCard(ctx context.Context, key string) (int64, error) {
	count, err := i.client.ZCard(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis zcard error: %w", err)
	}
	return count, nil
}

// ZIncrBy 增加成员的分数
func (i *impl) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	score, err := i.client.ZIncrBy(ctx, i.buildKey(key), increment, member).Result()
	if err != nil {
		return 0, fmt.Errorf("redis zincrby error: %w", err)
	}
	return score, nil
}

// ==================== Hash 操作实现 ====================

// HSet 设置哈希表字段的值
func (i *impl) HSet(ctx context.Context, key string, field string, value interface{}) error {
	err := i.client.HSet(ctx, i.buildKey(key), field, value).Err()
	if err != nil {
		return fmt.Errorf("redis hset error: %w", err)
	}
	return nil
}

// HGet 获取哈希表字段的值
func (i *impl) HGet(ctx context.Context, key, field string) (string, error) {
	value, err := i.client.HGet(ctx, i.buildKey(key), field).Result()
	if err == redis.Nil {
		return "", cache.ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("redis hget error: %w", err)
	}
	return value, nil
}

// HMSet 批量设置哈希表字段
func (i *impl) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	err := i.client.HMSet(ctx, i.buildKey(key), fields).Err()
	if err != nil {
		return fmt.Errorf("redis hmset error: %w", err)
	}
	return nil
}

// HMGet 批量获取哈希表字段
func (i *impl) HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	values, err := i.client.HMGet(ctx, i.buildKey(key), fields...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hmget error: %w", err)
	}
	return values, nil
}

// HGetAll 获取哈希表所有字段和值
func (i *impl) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	fields, err := i.client.HGetAll(ctx, i.buildKey(key)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hgetall error: %w", err)
	}
	return fields, nil
}

// HDel 删除哈希表字段
func (i *impl) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	count, err := i.client.HDel(ctx, i.buildKey(key), fields...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis hdel error: %w", err)
	}
	return count, nil
}

// HExists 判断哈希表字段是否存在
func (i *impl) HExists(ctx context.Context, key, field string) (bool, error) {
	exists, err := i.client.HExists(ctx, i.buildKey(key), field).Result()
	if err != nil {
		return false, fmt.Errorf("redis hexists error: %w", err)
	}
	return exists, nil
}

// HLen 获取哈希表字段数量
func (i *impl) HLen(ctx context.Context, key string) (int64, error) {
	length, err := i.client.HLen(ctx, i.buildKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis hlen error: %w", err)
	}
	return length, nil
}

// HKeys 获取哈希表所有字段名
func (i *impl) HKeys(ctx context.Context, key string) ([]string, error) {
	keys, err := i.client.HKeys(ctx, i.buildKey(key)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hkeys error: %w", err)
	}
	return keys, nil
}

// HVals 获取哈希表所有值
func (i *impl) HVals(ctx context.Context, key string) ([]string, error) {
	values, err := i.client.HVals(ctx, i.buildKey(key)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hvals error: %w", err)
	}
	return values, nil
}

// HIncrBy 增加哈希表字段的整数值
func (i *impl) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	value, err := i.client.HIncrBy(ctx, i.buildKey(key), field, incr).Result()
	if err != nil {
		return 0, fmt.Errorf("redis hincrby error: %w", err)
	}
	return value, nil
}

// 编译时检查，确保 impl 实现了 cache.Cache 接口
var _ cache.Cache = (*impl)(nil)
