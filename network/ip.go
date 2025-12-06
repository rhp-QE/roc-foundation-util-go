// Package network 提供网络相关的工具函数
//
// Author: Ruan Huipeng
// Date: 2025-12-01

package network

import (
	"fmt"
	"net"
)

// GetLocalIP 获取本机 IP 地址
// 返回第一个非回环的 IPv4 地址
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %w", err)
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no non-loopback IP found")
}

// GetLocalIPv6 获取本机 IPv6 地址
func GetLocalIPv6() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %w", err)
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To16() != nil && ipNet.IP.To4() == nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no non-loopback IPv6 found")
}

// GetLocalIPs 获取本机所有非回环 IP 地址
func GetLocalIPs() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("failed to get interface addresses: %w", err)
	}

	ips := make([]string, 0)
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() {
			ips = append(ips, ipNet.IP.String())
		}
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("no non-loopback IP found")
	}

	return ips, nil
}

// IsValidIP 验证 IP 地址是否有效
func IsValidIP(ip string) bool {
	return net.ParseIP(ip) != nil
}

// IsPrivateIP 检查 IP 是否为私有地址
func IsPrivateIP(ip string) bool {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}

	// 检查是否为私有 IP 地址
	// 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
	return parsedIP.IsLoopback() ||
		parsedIP.IsLinkLocalUnicast() ||
		parsedIP.IsLinkLocalMulticast() ||
		parsedIP.IsPrivate()
}
