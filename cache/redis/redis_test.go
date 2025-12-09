package redis

import (
	"context"
	"testing"
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/cache"
)

// TestNewRedisCache 测试创建 Redis 缓存
func TestNewRedisCache(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(0),
		},
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	err = c.Ping(ctx)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// TestStringOperations 测试 String 操作
func TestStringOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()

	// Set & Get
	key := "string_key"
	value := "test_value"
	err = c.Set(ctx, key, value, 1*time.Minute)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != value {
		t.Errorf("Expected %s, got %s", value, got)
	}

	// Delete
	err = c.Delete(ctx, key)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// Get after delete
	_, err = c.Get(ctx, key)
	if err != cache.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

// TestListOperations 测试 List 操作
func TestListOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	key := "list_key"

	// RPush
	count, err := c.RPush(ctx, key, "item1", "item2", "item3")
	if err != nil {
		t.Fatalf("RPush failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// LLen
	length, err := c.LLen(ctx, key)
	if err != nil {
		t.Fatalf("LLen failed: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	// LRange
	items, err := c.LRange(ctx, key, 0, -1)
	if err != nil {
		t.Fatalf("LRange failed: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(items))
	}

	// LPop
	item, err := c.LPop(ctx, key)
	if err != nil {
		t.Fatalf("LPop failed: %v", err)
	}
	if item != "item1" {
		t.Errorf("Expected 'item1', got %s", item)
	}

	// 清理
	c.Delete(ctx, key)
}

// TestSetOperations 测试 Set 操作
func TestSetOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	key := "set_key"

	// SAdd
	count, err := c.SAdd(ctx, key, "member1", "member2", "member3")
	if err != nil {
		t.Fatalf("SAdd failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// SCard
	cardCount, err := c.SCard(ctx, key)
	if err != nil {
		t.Fatalf("SCard failed: %v", err)
	}
	if cardCount != 3 {
		t.Errorf("Expected card 3, got %d", cardCount)
	}

	// SIsMember
	isMember, err := c.SIsMember(ctx, key, "member1")
	if err != nil {
		t.Fatalf("SIsMember failed: %v", err)
	}
	if !isMember {
		t.Error("Expected member1 to be in set")
	}

	// SMembers
	members, err := c.SMembers(ctx, key)
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// 清理
	c.Delete(ctx, key)
}

// TestHashOperations 测试 Hash 操作
func TestHashOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	key := "hash_key"

	// HSet
	err = c.HSet(ctx, key, "field1", "value1")
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	// HGet
	value, err := c.HGet(ctx, key, "field1")
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got %s", value)
	}

	// HMSet
	fields := map[string]interface{}{
		"field2": "value2",
		"field3": "value3",
	}
	err = c.HMSet(ctx, key, fields)
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	// HGetAll
	allFields, err := c.HGetAll(ctx, key)
	if err != nil {
		t.Fatalf("HGetAll failed: %v", err)
	}
	if len(allFields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(allFields))
	}

	// HLen
	length, err := c.HLen(ctx, key)
	if err != nil {
		t.Fatalf("HLen failed: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	// 清理
	c.Delete(ctx, key)
}

// TestSortedSetOperations 测试 Sorted Set 操作
func TestSortedSetOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	key := "zset_key"

	// ZAdd
	members := []cache.ZMember{
		{Score: 100, Member: "Alice"},
		{Score: 85, Member: "Bob"},
		{Score: 95, Member: "Charlie"},
	}
	count, err := c.ZAdd(ctx, key, members...)
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// ZCard
	cardCount, err := c.ZCard(ctx, key)
	if err != nil {
		t.Fatalf("ZCard failed: %v", err)
	}
	if cardCount != 3 {
		t.Errorf("Expected card 3, got %d", cardCount)
	}

	// ZScore
	score, err := c.ZScore(ctx, key, "Alice")
	if err != nil {
		t.Fatalf("ZScore failed: %v", err)
	}
	if score != 100 {
		t.Errorf("Expected score 100, got %f", score)
	}

	// ZRevRange (分数从高到低)
	top, err := c.ZRevRange(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("ZRevRange failed: %v", err)
	}
	if len(top) != 1 || top[0] != "Alice" {
		t.Errorf("Expected top member 'Alice', got %v", top)
	}

	// 清理
	c.Delete(ctx, key)
}

// TestIncrOperations 测试计数器操作
func TestIncrOperations(t *testing.T) {
	var (
		c   cache.Cache
		err error
	)
	c, err = NewRedisCache(
		[]Option{
			WithAddress("localhost:6379"),
			WithPassword("redis123"),
			WithDB(1),
		},
		cache.WithKeyPrefix("test:"),
	)
	if err != nil {
		t.Skipf("Skipping test, redis not available: %v", err)
		return
	}
	defer c.Close()

	ctx := context.Background()
	key := "counter"

	// Incr
	val, err := c.Incr(ctx, key)
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// IncrBy
	val, err = c.IncrBy(ctx, key, 10)
	if err != nil {
		t.Fatalf("IncrBy failed: %v", err)
	}
	if val != 11 {
		t.Errorf("Expected 11, got %d", val)
	}

	// Decr
	val, err = c.Decr(ctx, key)
	if err != nil {
		t.Fatalf("Decr failed: %v", err)
	}
	if val != 10 {
		t.Errorf("Expected 10, got %d", val)
	}

	// 清理
	c.Delete(ctx, key)
}
