package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/service_registry/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdRegistry 基于 etcd 的服务注册中心
type EtcdRegistry struct {
	client *clientv3.Client
	config *Config

	// 租约管理
	leases      map[string]clientv3.LeaseID // instanceID -> leaseID
	leasesMu    sync.RWMutex
	leaseDone   map[string]chan struct{} // instanceID -> done channel
	leaseDoneMu sync.RWMutex

	// 监听管理
	watchers   map[string][]chan []registry.ServiceInstance
	watchersMu sync.RWMutex

	// 订阅管理
	subscribers   map[string][]registry.InstanceChangeHandler
	subscribersMu sync.RWMutex
}

// NewEtcdRegistry 创建 etcd 注册中心
func NewEtcdRegistry(options ...Option) (*EtcdRegistry, error) {
	config := DefaultConfig()
	for _, opt := range options {
		opt(config)
	}

	client, err := clientv3.New(config.ToEtcdConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	r := &EtcdRegistry{
		client:      client,
		config:      config,
		leases:      make(map[string]clientv3.LeaseID),
		leaseDone:   make(map[string]chan struct{}),
		watchers:    make(map[string][]chan []registry.ServiceInstance),
		subscribers: make(map[string][]registry.InstanceChangeHandler),
	}

	return r, nil
}

// Register 注册服务实例
func (r *EtcdRegistry) Register(ctx context.Context, instance *registry.ServiceInstance) error {
	if instance == nil {
		return fmt.Errorf("instance cannot be nil")
	}

	// 创建租约
	leaseResp, err := r.client.Grant(ctx, r.config.TTL)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	// 保存租约ID
	r.leasesMu.Lock()
	r.leases[instance.InstanceID] = leaseResp.ID
	r.leasesMu.Unlock()

	// 序列化实例信息
	instance.RegisterAt = time.Now()
	instance.UpdateAt = time.Now()
	instanceData, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("failed to marshal instance: %w", err)
	}

	// 构建 key
	key := r.buildInstanceKey(instance.ServiceName, instance.InstanceID)

	// 注册到 etcd
	_, err = r.client.Put(ctx, key, string(instanceData), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return fmt.Errorf("failed to register instance: %w", err)
	}

	// 启动自动续约
	r.keepAlive(instance.InstanceID, leaseResp.ID)

	return nil
}

// Deregister 注销服务实例
func (r *EtcdRegistry) Deregister(ctx context.Context, instance *registry.ServiceInstance) error {
	if instance == nil {
		return fmt.Errorf("instance cannot be nil")
	}

	// 停止续约
	r.leaseDoneMu.Lock()
	if done, exists := r.leaseDone[instance.InstanceID]; exists {
		close(done)
		delete(r.leaseDone, instance.InstanceID)
	}
	r.leaseDoneMu.Unlock()

	// 删除租约
	r.leasesMu.Lock()
	if leaseID, exists := r.leases[instance.InstanceID]; exists {
		_, _ = r.client.Revoke(ctx, leaseID)
		delete(r.leases, instance.InstanceID)
	}
	r.leasesMu.Unlock()

	// 删除 key
	key := r.buildInstanceKey(instance.ServiceName, instance.InstanceID)
	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to deregister instance: %w", err)
	}

	return nil
}

// Discover 发现服务实例
func (r *EtcdRegistry) Discover(ctx context.Context, serviceName string) ([]registry.ServiceInstance, error) {
	prefix := r.buildServicePrefix(serviceName)

	// 获取所有实例
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to discover instances: %w", err)
	}

	instances := make([]registry.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var instance registry.ServiceInstance
		if err := json.Unmarshal(kv.Value, &instance); err != nil {
			continue
		}
		instances = append(instances, instance)
	}

	return instances, nil
}

// Watch 监听服务变化
func (r *EtcdRegistry) Watch(ctx context.Context, serviceName string) (<-chan []registry.ServiceInstance, error) {
	ch := make(chan []registry.ServiceInstance, 10)

	// 保存通道
	r.watchersMu.Lock()
	r.watchers[serviceName] = append(r.watchers[serviceName], ch)
	r.watchersMu.Unlock()

	// 启动监听协程
	go r.watchService(ctx, serviceName, ch)

	return ch, nil
}

// Subscribe 订阅服务变化
func (r *EtcdRegistry) Subscribe(ctx context.Context, serviceName string, handler registry.InstanceChangeHandler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// 保存订阅者
	r.subscribersMu.Lock()
	r.subscribers[serviceName] = append(r.subscribers[serviceName], handler)
	r.subscribersMu.Unlock()

	// 启动监听协程
	go r.watchServiceWithHandler(ctx, serviceName, handler)

	return nil
}

// Heartbeat 发送心跳
func (r *EtcdRegistry) Heartbeat(ctx context.Context, instance *registry.ServiceInstance) error {
	r.leasesMu.RLock()
	leaseID, exists := r.leases[instance.InstanceID]
	r.leasesMu.RUnlock()

	if !exists {
		return fmt.Errorf("lease not found for instance %s", instance.InstanceID)
	}

	_, err := r.client.KeepAliveOnce(ctx, leaseID)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	return nil
}

// UpdateStatus 更新实例状态
func (r *EtcdRegistry) UpdateStatus(ctx context.Context, instance *registry.ServiceInstance, status registry.InstanceStatus) error {
	instance.Status = status
	instance.UpdateAt = time.Now()
	return r.updateInstance(ctx, instance)
}

// UpdateMetadata 更新实例元数据
func (r *EtcdRegistry) UpdateMetadata(ctx context.Context, instance *registry.ServiceInstance, metadata map[string]string) error {
	instance.Metadata = metadata
	instance.UpdateAt = time.Now()
	return r.updateInstance(ctx, instance)
}

// GetService 获取指定服务的详细信息
func (r *EtcdRegistry) GetService(ctx context.Context, serviceName string, instanceID string) (*registry.ServiceInstance, error) {
	key := r.buildInstanceKey(serviceName, instanceID)

	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("instance not found")
	}

	var instance registry.ServiceInstance
	if err := json.Unmarshal(resp.Kvs[0].Value, &instance); err != nil {
		return nil, fmt.Errorf("failed to unmarshal instance: %w", err)
	}

	return &instance, nil
}

// ListServices 列出所有服务名称
func (r *EtcdRegistry) ListServices(ctx context.Context) ([]string, error) {
	resp, err := r.client.Get(ctx, r.config.Namespace, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	serviceMap := make(map[string]bool)
	for _, kv := range resp.Kvs {
		// 解析 key 获取服务名
		// key 格式: /services/{serviceName}/{instanceID}
		key := string(kv.Key)
		parts := splitKey(key, r.config.Namespace)
		if len(parts) >= 2 {
			serviceMap[parts[0]] = true
		}
	}

	services := make([]string, 0, len(serviceMap))
	for service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

// Close 关闭注册中心连接
func (r *EtcdRegistry) Close() error {
	// 停止所有续约
	r.leaseDoneMu.Lock()
	for _, done := range r.leaseDone {
		close(done)
	}
	r.leaseDone = make(map[string]chan struct{})
	r.leaseDoneMu.Unlock()

	// 关闭所有监听通道
	r.watchersMu.Lock()
	for _, watchers := range r.watchers {
		for _, ch := range watchers {
			close(ch)
		}
	}
	r.watchers = make(map[string][]chan []registry.ServiceInstance)
	r.watchersMu.Unlock()

	// 给一点时间让后台任务优雅退出
	// 避免 "context canceled" 的警告日志
	time.Sleep(50 * time.Millisecond)

	// 关闭 etcd 客户端
	return r.client.Close()
}

// HealthCheck 健康检查
func (r *EtcdRegistry) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := r.client.Get(ctx, r.config.Namespace)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// keepAlive 保持租约活跃
func (r *EtcdRegistry) keepAlive(instanceID string, leaseID clientv3.LeaseID) {
	done := make(chan struct{})

	r.leaseDoneMu.Lock()
	r.leaseDone[instanceID] = done
	r.leaseDoneMu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Duration(r.config.HeartbeatInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_, err := r.client.KeepAliveOnce(context.Background(), leaseID)
				if err != nil {
					// 租约可能已失效，尝试清理
					r.leasesMu.Lock()
					delete(r.leases, instanceID)
					r.leasesMu.Unlock()
					return
				}
			}
		}
	}()
}

// watchService 监听服务变化
func (r *EtcdRegistry) watchService(ctx context.Context, serviceName string, ch chan []registry.ServiceInstance) {
	prefix := r.buildServicePrefix(serviceName)
	watchChan := r.client.Watch(ctx, prefix, clientv3.WithPrefix())

	// 先发送当前实例列表
	instances, _ := r.Discover(ctx, serviceName)
	select {
	case ch <- instances:
	case <-ctx.Done():
		return
	}

	// 监听变化
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp := <-watchChan:
			if watchResp.Err() != nil {
				continue
			}

			// 获取最新实例列表
			instances, err := r.Discover(ctx, serviceName)
			if err != nil {
				continue
			}

			select {
			case ch <- instances:
			case <-ctx.Done():
				return
			}
		}
	}
}

// watchServiceWithHandler 监听服务变化并调用处理函数
func (r *EtcdRegistry) watchServiceWithHandler(ctx context.Context, serviceName string, handler registry.InstanceChangeHandler) {
	prefix := r.buildServicePrefix(serviceName)
	watchChan := r.client.Watch(ctx, prefix, clientv3.WithPrefix())

	// 先发送当前实例列表
	instances, _ := r.Discover(ctx, serviceName)
	handler(serviceName, instances)

	// 监听变化
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp := <-watchChan:
			if watchResp.Err() != nil {
				continue
			}

			// 获取最新实例列表
			instances, err := r.Discover(ctx, serviceName)
			if err != nil {
				continue
			}

			handler(serviceName, instances)
		}
	}
}

// updateInstance 更新实例信息
func (r *EtcdRegistry) updateInstance(ctx context.Context, instance *registry.ServiceInstance) error {
	r.leasesMu.RLock()
	leaseID, exists := r.leases[instance.InstanceID]
	r.leasesMu.RUnlock()

	if !exists {
		return fmt.Errorf("instance not registered: %s", instance.InstanceID)
	}

	// 序列化实例信息
	instanceData, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("failed to marshal instance: %w", err)
	}

	// 更新到 etcd
	key := r.buildInstanceKey(instance.ServiceName, instance.InstanceID)
	_, err = r.client.Put(ctx, key, string(instanceData), clientv3.WithLease(leaseID))
	if err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}

	return nil
}

// buildServicePrefix 构建服务前缀
func (r *EtcdRegistry) buildServicePrefix(serviceName string) string {
	return path.Join(r.config.Namespace, serviceName) + "/"
}

// buildInstanceKey 构建实例 key
func (r *EtcdRegistry) buildInstanceKey(serviceName, instanceID string) string {
	return path.Join(r.config.Namespace, serviceName, instanceID)
}

// splitKey 分割 key
func splitKey(key, namespace string) []string {
	// 移除命名空间前缀
	key = key[len(namespace):]
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	parts := []string{}
	start := 0
	for i, c := range key {
		if c == '/' {
			if i > start {
				parts = append(parts, key[start:i])
			}
			start = i + 1
		}
	}
	if start < len(key) {
		parts = append(parts, key[start:])
	}

	return parts
}

// SetNamespace 设置命名空间
func (r *EtcdRegistry) SetNamespace(namespace string) {
	r.config.Namespace = namespace
}

// SetTimeout 设置超时时间
func (r *EtcdRegistry) SetTimeout(timeout int) {
	r.config.DialTimeout = time.Duration(timeout) * time.Second
}

// SetTTL 设置 TTL
func (r *EtcdRegistry) SetTTL(ttl int64) {
	r.config.TTL = ttl
}



// 1. 后端服务启动
//    ↓
// 2. 读取配置文件，获取初始 etcd 节点
//    ["etcd-1:2379", "etcd-2:2379", "etcd-3:2379"]
//    ↓
// 3. 创建 etcd 客户端（使用初始节点）
//    ↓
// 4. 连接配置中心，获取最新节点列表（可选）
//    ↓
// 5. 订阅配置中心变更
//    ↓
// 6. 服务正常运行...
//    ↓
// 7. etcd 集群新增节点（etcd-4, etcd-5）
//    ↓
// 8. 管理服务检测到变化（或手动更新）
//    ↓
// 9. 管理服务推送新节点列表到配置中心
//    ↓
// 10. 配置中心通知所有订阅的后端服务
//     ↓
// 11. 后端服务更新 etcd 客户端节点列表
//     client.SetEndpoints(newNodes)
//     ↓
// 12. 后端服务开始使用新节点