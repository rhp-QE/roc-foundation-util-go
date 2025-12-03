package redis

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Config Redis 配置
type Config struct {
	// Address Redis 地址
	Address string

	// Password 密码
	Password string

	// DB 数据库编号
	DB int

	// PoolSize 连接池大小
	PoolSize int

	// MinIdleConns 最小空闲连接数
	MinIdleConns int

	// MaxRetries 最大重试次数
	MaxRetries int

	// DialTimeout 连接超时
	DialTimeout time.Duration

	// ReadTimeout 读超时
	ReadTimeout time.Duration

	// WriteTimeout 写超时
	WriteTimeout time.Duration

	// PoolTimeout 连接池超时
	PoolTimeout time.Duration
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Address:      "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 2,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	}
}

// Option 配置选项
type Option func(*Config)

// WithAddress 设置地址
func WithAddress(address string) Option {
	return func(c *Config) {
		c.Address = address
	}
}

// WithPassword 设置密码
func WithPassword(password string) Option {
	return func(c *Config) {
		c.Password = password
	}
}

// WithDB 设置数据库编号
func WithDB(db int) Option {
	return func(c *Config) {
		c.DB = db
	}
}

// WithPoolSize 设置连接池大小
func WithPoolSize(size int) Option {
	return func(c *Config) {
		c.PoolSize = size
	}
}

// WithTimeout 设置超时时间
func WithTimeout(dial, read, write time.Duration) Option {
	return func(c *Config) {
		c.DialTimeout = dial
		c.ReadTimeout = read
		c.WriteTimeout = write
	}
}

// ToRedisOptions 转换为 redis.Options
func (c *Config) ToRedisOptions() *redis.Options {
	return &redis.Options{
		Addr:         c.Address,
		Password:     c.Password,
		DB:           c.DB,
		PoolSize:     c.PoolSize,
		MinIdleConns: c.MinIdleConns,
		MaxRetries:   c.MaxRetries,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		PoolTimeout:  c.PoolTimeout,
	}
}

