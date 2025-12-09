package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rhp-QE/roc-foundation-util-go/service_registry/discovery"
	"github.com/rhp-QE/roc-foundation-util-go/service_registry/loadbalancer"
	"github.com/rhp-QE/roc-foundation-util-go/service_registry/registry"
	"github.com/rhp-QE/roc-foundation-util-go/service_registry/registry/etcd"
)

func main() {
	// 创建 etcd 注册中心
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/demo/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		log.Fatalf("Failed to create registry: %v", err)
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册多个服务实例
	instances := []*registry.ServiceInstance{
		{
			ServiceName: "api-gateway",
			InstanceID:  "api-gateway-001",
			Host:        "192.168.1.101",
			Port:        8080,
			Status:      registry.StatusHealthy,
			Weight:      100,
			Version:     "v1.0.0",
			Metadata: map[string]string{
				"region": "beijing",
				"zone":   "zone-a",
			},
		},
		{
			ServiceName: "api-gateway",
			InstanceID:  "api-gateway-002",
			Host:        "192.168.1.102",
			Port:        8080,
			Status:      registry.StatusHealthy,
			Weight:      150,
			Version:     "v1.0.0",
			Metadata: map[string]string{
				"region": "beijing",
				"zone":   "zone-b",
			},
		},
		{
			ServiceName: "api-gateway",
			InstanceID:  "api-gateway-003",
			Host:        "192.168.1.103",
			Port:        8080,
			Status:      registry.StatusHealthy,
			Weight:      80,
			Version:     "v1.1.0",
			Metadata: map[string]string{
				"region": "shanghai",
				"zone":   "zone-a",
			},
		},
	}

	// 注册所有实例
	for _, instance := range instances {
		err = reg.Register(ctx, instance)
		if err != nil {
			log.Printf("Failed to register %s: %v", instance.InstanceID, err)
			continue
		}
		fmt.Printf("Registered: %s (%s:%d) weight=%d\n",
			instance.InstanceID, instance.Host, instance.Port, instance.Weight)
	}

	// 等待注册完成
	time.Sleep(2 * time.Second)

	// 演示不同的负载均衡策略
	demonstrateLoadBalancers(ctx, reg)

	// 保持运行
	fmt.Println("\nServices are running... Press Ctrl+C to exit")
	time.Sleep(30 * time.Second)

	// 注销所有服务
	for _, instance := range instances {
		err = reg.Deregister(ctx, instance)
		if err != nil {
			log.Printf("Failed to deregister %s: %v", instance.InstanceID, err)
		}
	}
	fmt.Println("All services deregistered")
}

func demonstrateLoadBalancers(ctx context.Context, reg registry.Registry) {
	serviceName := "api-gateway"

	// 获取所有实例
	instances, err := reg.Discover(ctx, serviceName)
	if err != nil {
		log.Printf("Failed to discover service: %v", err)
		return
	}

	fmt.Printf("\n=== Discovered %d instances ===\n", len(instances))
	for _, inst := range instances {
		fmt.Printf("  %s: %s:%d (weight=%d, version=%s)\n",
			inst.InstanceID, inst.Host, inst.Port, inst.Weight, inst.Version)
	}

	// 1. 轮询负载均衡
	fmt.Println("\n=== Round Robin Load Balancer ===")
	rrLB := loadbalancer.NewRoundRobinLoadBalancer()
	client := discovery.NewDiscovery(reg, rrLB)
	defer client.Close()

	for i := 0; i < 6; i++ {
		instance, err := client.GetInstance(ctx, serviceName)
		if err != nil {
			log.Printf("Failed to get instance: %v", err)
			continue
		}
		fmt.Printf("Request %d -> %s (%s:%d)\n", i+1, instance.InstanceID, instance.Host, instance.Port)
	}

	// 2. 随机负载均衡
	fmt.Println("\n=== Random Load Balancer ===")
	randomLB := loadbalancer.NewRandomLoadBalancer()
	client2 := discovery.NewDiscovery(reg, randomLB)
	defer client2.Close()

	for i := 0; i < 6; i++ {
		instance, err := client2.GetInstance(ctx, serviceName)
		if err != nil {
			log.Printf("Failed to get instance: %v", err)
			continue
		}
		fmt.Printf("Request %d -> %s (%s:%d)\n", i+1, instance.InstanceID, instance.Host, instance.Port)
	}

	// 3. 加权随机负载均衡
	fmt.Println("\n=== Weighted Random Load Balancer ===")
	weightedLB := loadbalancer.NewWeightedRandomLoadBalancer()
	client3 := discovery.NewDiscovery(reg, weightedLB)
	defer client3.Close()

	for i := 0; i < 10; i++ {
		instance, err := client3.GetInstance(ctx, serviceName)
		if err != nil {
			log.Printf("Failed to get instance: %v", err)
			continue
		}
		fmt.Printf("Request %d -> %s (%s:%d) weight=%d\n",
			i+1, instance.InstanceID, instance.Host, instance.Port, instance.Weight)
	}

	// 4. 一致性哈希负载均衡
	fmt.Println("\n=== Consistent Hash Load Balancer ===")
	hashLB := loadbalancer.NewConsistentHashLoadBalancer(150)
	client4 := discovery.NewDiscovery(reg, hashLB)
	defer client4.Close()

	// 使用不同的 key 测试一致性哈希
	userIDs := []string{"user-001", "user-002", "user-003", "user-001", "user-002", "user-003"}
	for _, userID := range userIDs {
		instance, err := client4.GetInstance(ctx, serviceName, userID)
		if err != nil {
			log.Printf("Failed to get instance: %v", err)
			continue
		}
		fmt.Printf("User %s -> %s (%s:%d)\n",
			userID, instance.InstanceID, instance.Host, instance.Port)
	}
}
