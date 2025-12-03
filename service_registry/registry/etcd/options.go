package etcd

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config etcd 注册中心配置
type Config struct {
	// Endpoints etcd 服务器地址列表
	Endpoints []string

	// DialTimeout 连接超时时间
	DialTimeout time.Duration

	// Username 用户名（可选）
	Username string

	// Password 密码（可选）
	Password string

	// Namespace 命名空间前缀
	Namespace string

	// TTL 服务实例的生存时间（秒）
	TTL int64

	// HeartbeatInterval 心跳间隔（秒）
	HeartbeatInterval int64
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Endpoints:         []string{"localhost:2379"},
		DialTimeout:       5 * time.Second,
		Namespace:         "/services",
		TTL:               10,
		HeartbeatInterval: 3,
	}
}

// Option 配置选项函数
type Option func(*Config)

// WithEndpoints 设置 etcd 服务器地址
func WithEndpoints(endpoints []string) Option {
	return func(c *Config) {
		c.Endpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时时间
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.DialTimeout = timeout
	}
}

// WithAuth 设置认证信息
func WithAuth(username, password string) Option {
	return func(c *Config) {
		c.Username = username
		c.Password = password
	}
}

// WithNamespace 设置命名空间
func WithNamespace(namespace string) Option {
	return func(c *Config) {
		c.Namespace = namespace
	}
}

// WithTTL 设置 TTL
func WithTTL(ttl int64) Option {
	return func(c *Config) {
		c.TTL = ttl
	}
}

// WithHeartbeatInterval 设置心跳间隔
func WithHeartbeatInterval(interval int64) Option {
	return func(c *Config) {
		c.HeartbeatInterval = interval
	}
}

// ToEtcdConfig 转换为 etcd client 配置
func (c *Config) ToEtcdConfig() clientv3.Config {
	config := clientv3.Config{
		Endpoints:   c.Endpoints,
		DialTimeout: c.DialTimeout,
	}

	if c.Username != "" && c.Password != "" {
		config.Username = c.Username
		config.Password = c.Password
	}

	// 默认使用静默 logger（只显示 Error 级别）
	// 这样可以过滤掉 "context canceled" 等正常的 warn 日志
	config.Logger = GetQuietLogger()

	return config
}
