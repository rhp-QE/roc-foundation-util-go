package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建服务发现客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := discovery.NewDiscovery(reg, lb,
		discovery.WithCache(true),
		discovery.WithWatch(true),
	)
	defer client.Close()

	serviceName := "order-service"

	// 订阅服务变化
	fmt.Printf("Watching service: %s\n\n", serviceName)
	err = client.Subscribe(ctx, serviceName, func(svcName string, instances []registry.ServiceInstance) {
		fmt.Printf("[%s] Service '%s' changed:\n", time.Now().Format("15:04:05"), svcName)
		if len(instances) == 0 {
			fmt.Println("  No instances available")
		} else {
			for i, inst := range instances {
				fmt.Printf("  [%d] %s - %s:%d (status: %s, version: %s)\n",
					i+1, inst.InstanceID, inst.Host, inst.Port, inst.Status, inst.Version)
			}
		}
		fmt.Println()
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// 启动模拟服务注册/注销的协程
	go simulateServiceChanges(ctx, reg, serviceName)

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Press Ctrl+C to exit...")
	<-sigChan
	fmt.Println("\nShutting down...")
}

func simulateServiceChanges(ctx context.Context, reg registry.Registry, serviceName string) {
	instances := []*registry.ServiceInstance{
		{
			ServiceName: serviceName,
			InstanceID:  "order-service-001",
			Host:        "192.168.1.101",
			Port:        9001,
			Status:      registry.StatusHealthy,
			Weight:      100,
			Version:     "v1.0.0",
		},
		{
			ServiceName: serviceName,
			InstanceID:  "order-service-002",
			Host:        "192.168.1.102",
			Port:        9002,
			Status:      registry.StatusHealthy,
			Weight:      100,
			Version:     "v1.0.0",
		},
		{
			ServiceName: serviceName,
			InstanceID:  "order-service-003",
			Host:        "192.168.1.103",
			Port:        9003,
			Status:      registry.StatusHealthy,
			Weight:      100,
			Version:     "v1.1.0",
		},
	}

	// 每5秒注册一个实例
	for i, instance := range instances {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			err := reg.Register(ctx, instance)
			if err != nil {
				log.Printf("Failed to register %s: %v", instance.InstanceID, err)
			} else {
				fmt.Printf("[Action] Registered instance %d: %s\n\n", i+1, instance.InstanceID)
			}
		}
	}

	// 等待10秒
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	// 更新第一个实例的状态
	instance := instances[0]
	err := reg.UpdateStatus(ctx, instance, registry.StatusUnhealthy)
	if err != nil {
		log.Printf("Failed to update status: %v", err)
	} else {
		fmt.Printf("[Action] Updated %s status to unhealthy\n\n", instance.InstanceID)
	}

	// 等待10秒
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	// 恢复第一个实例的状态
	err = reg.UpdateStatus(ctx, instance, registry.StatusHealthy)
	if err != nil {
		log.Printf("Failed to update status: %v", err)
	} else {
		fmt.Printf("[Action] Updated %s status to healthy\n\n", instance.InstanceID)
	}

	// 等待10秒
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	// 注销第二个实例
	instance = instances[1]
	err = reg.Deregister(ctx, instance)
	if err != nil {
		log.Printf("Failed to deregister %s: %v", instance.InstanceID, err)
	} else {
		fmt.Printf("[Action] Deregistered instance: %s\n\n", instance.InstanceID)
	}

	// 保持运行
	<-ctx.Done()

	// 清理：注销所有实例
	for _, inst := range instances {
		_ = reg.Deregister(context.Background(), inst)
	}
}
