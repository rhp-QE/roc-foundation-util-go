package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config MongoDB 配置
type Config struct {
	// URI MongoDB 连接字符串
	URI string

	// Database 数据库名称
	Database string

	// Username 用户名
	Username string

	// Password 密码
	Password string

	// MaxPoolSize 最大连接池大小
	MaxPoolSize uint64

	// MinPoolSize 最小连接池大小
	MinPoolSize uint64

	// MaxConnIdleTime 连接最大空闲时间
	MaxConnIdleTime time.Duration

	// Timeout 操作超时时间
	Timeout time.Duration
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		URI:             "mongodb://localhost:27017",
		Database:        "test",
		MaxPoolSize:     100,
		MinPoolSize:     10,
		MaxConnIdleTime: 10 * time.Minute,
		Timeout:         10 * time.Second,
	}
}

// Option 配置选项
type Option func(*Config)

// WithURI 设置连接字符串
func WithURI(uri string) Option {
	return func(c *Config) {
		c.URI = uri
	}
}

// WithDatabase 设置数据库名称
func WithDatabase(database string) Option {
	return func(c *Config) {
		c.Database = database
	}
}

// WithAuth 设置认证信息
func WithAuth(username, password string) Option {
	return func(c *Config) {
		c.Username = username
		c.Password = password
	}
}

// WithPoolSize 设置连接池大小
func WithPoolSize(min, max uint64) Option {
	return func(c *Config) {
		c.MinPoolSize = min
		c.MaxPoolSize = max
	}
}

// WithTimeout 设置超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// ToMongoOptions 转换为 MongoDB 客户端选项
func (c *Config) ToMongoOptions() *options.ClientOptions {
	clientOpts := options.Client().
		ApplyURI(c.URI).
		SetMaxPoolSize(c.MaxPoolSize).
		SetMinPoolSize(c.MinPoolSize).
		SetMaxConnIdleTime(c.MaxConnIdleTime)

	if c.Username != "" && c.Password != "" {
		clientOpts.SetAuth(options.Credential{
			Username: c.Username,
			Password: c.Password,
		})
	}

	return clientOpts
}

