package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/service_registry/loadbalancer"
	"github.com/rhp-QE/roc-foundation-util-go/service_registry/registry"
)

// Discovery 服务发现接口
//
// 定义了服务发现客户端的核心功能，让使用者快速了解所有可用方法。
//
// 使用示例：
//
//	reg := etcd.NewEtcdRegistry(...)
//	lb := loadbalancer.NewRoundRobinLoadBalancer()
//	disc := discovery.NewDiscovery(reg, lb,
//	    discovery.WithCache(true),
//	    discovery.WithWatch(true),
//	)
//	defer disc.Close()
//
//	// 获取服务实例
//	instance, err := disc.GetInstance(ctx, "user-service")
type Discovery interface {
	// GetInstance 获取单个服务实例（使用负载均衡算法选择）
	GetInstance(ctx context.Context, serviceName string, key ...string) (*registry.ServiceInstance, error)

	// GetInstances 获取所有服务实例
	GetInstances(ctx context.Context, serviceName string) ([]registry.ServiceInstance, error)

	// GetHealthyInstances 获取所有健康的服务实例
	GetHealthyInstances(ctx context.Context, serviceName string) ([]registry.ServiceInstance, error)

	// Subscribe 订阅服务变化
	Subscribe(ctx context.Context, serviceName string, handler registry.InstanceChangeHandler) error

	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, serviceName string) error

	// Refresh 刷新服务实例缓存
	Refresh(ctx context.Context, serviceName string) error

	// GetCachedServices 获取所有已缓存的服务
	GetCachedServices() []string

	// ClearCache 清空所有缓存
	ClearCache()

	// ClearServiceCache 清空指定服务的缓存
	ClearServiceCache(serviceName string)

	// GetConfig 获取当前配置
	GetConfig() *Config

	// SetConfig 更新配置
	SetConfig(config *Config)

	// Close 关闭客户端
	Close() error
}

// Config 服务发现配置
type Config struct {
	// CacheEnabled 是否启用缓存
	CacheEnabled bool

	// CacheTTL 缓存过期时间
	CacheTTL time.Duration

	// AutoRefresh 是否自动刷新缓存
	AutoRefresh bool

	// RefreshInterval 自动刷新间隔
	RefreshInterval time.Duration

	// EnableWatch 是否启用监听
	EnableWatch bool
}

// Option 配置选项
type Option func(*Config)

// WithCache 设置是否启用缓存
func WithCache(enabled bool) Option {
	return func(c *Config) {
		c.CacheEnabled = enabled
	}
}

// WithCacheTTL 设置缓存过期时间
func WithCacheTTL(ttl time.Duration) Option {
	return func(c *Config) {
		c.CacheTTL = ttl
	}
}

// WithAutoRefresh 设置是否自动刷新
func WithAutoRefresh(enabled bool, interval time.Duration) Option {
	return func(c *Config) {
		c.AutoRefresh = enabled
		c.RefreshInterval = interval
	}
}

// WithWatch 设置是否启用监听
func WithWatch(enabled bool) Option {
	return func(c *Config) {
		c.EnableWatch = enabled
	}
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		CacheEnabled:    true,
		CacheTTL:        30 * time.Second,
		AutoRefresh:     false,
		RefreshInterval: 10 * time.Second,
		EnableWatch:     true,
	}
}

// NewDiscovery 创建服务发现实例
//
// 参数：
//   - reg: 服务注册中心
//   - lb: 负载均衡器（如果为 nil，默认使用轮询）
//   - options: 配置选项
//
// 返回：
//   - Discovery 接口实例
func NewDiscovery(reg registry.Registry, lb loadbalancer.LoadBalancer, options ...Option) Discovery {
	config := DefaultConfig()
	for _, opt := range options {
		opt(config)
	}

	if lb == nil {
		// 默认使用轮询负载均衡
		lb = loadbalancer.NewRoundRobinLoadBalancer()
	}

	return &impl{
		registry:      reg,
		loadBalancer:  lb,
		cache:         make(map[string]*serviceCache),
		subscriptions: make(map[string]context.CancelFunc),
		config:        config,
	}
}

// impl 服务发现的内部实现 ================================================================================
//
// 实现 Discovery 接口，对外不暴露。
type impl struct {
	registry     registry.Registry
	loadBalancer loadbalancer.LoadBalancer

	// 服务实例缓存
	cache      map[string]*serviceCache
	cacheMutex sync.RWMutex

	// 订阅管理
	subscriptions   map[string]context.CancelFunc
	subscriptionsMu sync.Mutex

	// 配置
	config *Config
}

// serviceCache 服务缓存
type serviceCache struct {
	instances []registry.ServiceInstance
	updateAt  time.Time
}

// isExpired 判断缓存是否过期
func (c *serviceCache) isExpired(ttl time.Duration) bool {
	return time.Since(c.updateAt) > ttl
}

// GetInstance 获取服务实例
//
// 使用负载均衡算法从可用实例中选择一个实例。
// 支持传入 key 参数用于一致性哈希等算法。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//   - key: 可选的键（用于一致性哈希）
//
// 返回：
//   - 选中的服务实例
//   - 错误信息
func (i *impl) GetInstance(ctx context.Context, serviceName string, key ...string) (*registry.ServiceInstance, error) {
	instances, err := i.GetInstances(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	if len(instances) == 0 {
		return nil, fmt.Errorf("no available instances for service: %s", serviceName)
	}

	// 使用负载均衡器选择实例
	return i.loadBalancer.Select(instances, key...)
}

// GetInstances 获取所有服务实例
//
// 如果启用了缓存且缓存未过期，则从缓存返回。
// 否则从注册中心获取最新实例列表并更新缓存。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//
// 返回：
//   - 服务实例列表
//   - 错误信息
func (i *impl) GetInstances(ctx context.Context, serviceName string) ([]registry.ServiceInstance, error) {
	// 如果启用缓存，先从缓存获取
	if i.config.CacheEnabled {
		i.cacheMutex.RLock()
		cache, exists := i.cache[serviceName]
		i.cacheMutex.RUnlock()

		if exists && !cache.isExpired(i.config.CacheTTL) {
			return cache.instances, nil
		}
	}

	// 从注册中心获取
	instances, err := i.registry.Discover(ctx, serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to discover service %s: %w", serviceName, err)
	}

	// 更新缓存
	if i.config.CacheEnabled {
		i.cacheMutex.Lock()
		i.cache[serviceName] = &serviceCache{
			instances: instances,
			updateAt:  time.Now(),
		}
		i.cacheMutex.Unlock()
	}

	// 如果启用监听且未订阅，则自动订阅
	if i.config.EnableWatch {
		i.subscriptionsMu.Lock()
		if _, subscribed := i.subscriptions[serviceName]; !subscribed {
			i.subscriptionsMu.Unlock()
			_ = i.Subscribe(ctx, serviceName, i.defaultChangeHandler)
		} else {
			i.subscriptionsMu.Unlock()
		}
	}

	return instances, nil
}

// GetHealthyInstances 获取所有健康的服务实例
//
// 从所有实例中过滤出状态为健康的实例。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//
// 返回：
//   - 健康的服务实例列表
//   - 错误信息
func (i *impl) GetHealthyInstances(ctx context.Context, serviceName string) ([]registry.ServiceInstance, error) {
	instances, err := i.GetInstances(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	healthy := make([]registry.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		if instance.IsHealthy() {
			healthy = append(healthy, instance)
		}
	}

	return healthy, nil
}

// Subscribe 订阅服务变化
//
// 当服务实例发生变化时，会调用提供的处理函数。
// 如果已经存在订阅，会先取消旧的订阅。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//   - handler: 变化处理函数
//
// 返回：
//   - 错误信息
func (i *impl) Subscribe(ctx context.Context, serviceName string, handler registry.InstanceChangeHandler) error {
	i.subscriptionsMu.Lock()
	defer i.subscriptionsMu.Unlock()

	// 检查是否已订阅
	if cancel, exists := i.subscriptions[serviceName]; exists {
		cancel() // 取消旧的订阅
	}

	// 创建新的上下文
	subCtx, cancel := context.WithCancel(context.Background())
	i.subscriptions[serviceName] = cancel

	// 订阅服务变化
	err := i.registry.Subscribe(subCtx, serviceName, handler)
	if err != nil {
		delete(i.subscriptions, serviceName)
		cancel()
		return fmt.Errorf("failed to subscribe service %s: %w", serviceName, err)
	}

	return nil
}

// Unsubscribe 取消订阅
//
// 取消对指定服务的订阅。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//
// 返回：
//   - 错误信息
func (i *impl) Unsubscribe(ctx context.Context, serviceName string) error {
	i.subscriptionsMu.Lock()
	defer i.subscriptionsMu.Unlock()

	if cancel, exists := i.subscriptions[serviceName]; exists {
		cancel()
		delete(i.subscriptions, serviceName)
	}

	return nil
}

// Refresh 刷新服务实例缓存
//
// 强制从注册中心获取最新的服务实例列表并更新缓存。
//
// 参数：
//   - ctx: 上下文
//   - serviceName: 服务名称
//
// 返回：
//   - 错误信息
func (i *impl) Refresh(ctx context.Context, serviceName string) error {
	// 从注册中心获取最新实例
	instances, err := i.registry.Discover(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("failed to refresh service %s: %w", serviceName, err)
	}

	// 更新缓存
	i.cacheMutex.Lock()
	i.cache[serviceName] = &serviceCache{
		instances: instances,
		updateAt:  time.Now(),
	}
	i.cacheMutex.Unlock()

	return nil
}

// Close 关闭服务发现客户端
//
// 取消所有订阅并清空缓存。
//
// 返回：
//   - 错误信息
func (i *impl) Close() error {
	// 取消所有订阅
	i.subscriptionsMu.Lock()
	for _, cancel := range i.subscriptions {
		cancel()
	}
	i.subscriptions = make(map[string]context.CancelFunc)
	i.subscriptionsMu.Unlock()

	// 清空缓存
	i.cacheMutex.Lock()
	i.cache = make(map[string]*serviceCache)
	i.cacheMutex.Unlock()

	return nil
}

// defaultChangeHandler 默认的变更处理函数
func (i *impl) defaultChangeHandler(serviceName string, instances []registry.ServiceInstance) {
	// 更新缓存
	i.cacheMutex.Lock()
	i.cache[serviceName] = &serviceCache{
		instances: instances,
		updateAt:  time.Now(),
	}
	i.cacheMutex.Unlock()
}

// GetCachedServices 获取所有已缓存的服务
//
// 返回所有已缓存的服务名称列表。
//
// 返回：
//   - 服务名称列表
func (i *impl) GetCachedServices() []string {
	i.cacheMutex.RLock()
	defer i.cacheMutex.RUnlock()

	services := make([]string, 0, len(i.cache))
	for service := range i.cache {
		services = append(services, service)
	}

	return services
}

// ClearCache 清空所有缓存
func (i *impl) ClearCache() {
	i.cacheMutex.Lock()
	i.cache = make(map[string]*serviceCache)
	i.cacheMutex.Unlock()
}

// ClearServiceCache 清空指定服务的缓存
//
// 参数：
//   - serviceName: 服务名称
func (i *impl) ClearServiceCache(serviceName string) {
	i.cacheMutex.Lock()
	delete(i.cache, serviceName)
	i.cacheMutex.Unlock()
}

// GetConfig 获取当前配置
func (i *impl) GetConfig() *Config {
	i.cacheMutex.RLock()
	defer i.cacheMutex.RUnlock()

	// 返回配置的副本，避免外部修改
	configCopy := *i.config
	return &configCopy
}

// SetConfig 更新配置
func (i *impl) SetConfig(config *Config) {
	i.cacheMutex.Lock()
	defer i.cacheMutex.Unlock()

	if config != nil {
		i.config = config
	}
}

// 编译时检查，确保 impl 实现了 Discovery 接口
var _ Discovery = (*impl)(nil)
