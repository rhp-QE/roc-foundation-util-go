// Package stringutil 提供字符串处理的工具函数
//
// Author: Ruan Huipeng
// Date: 2025-12-01

package stringutil

import (
	"strings"
)

// IsEmpty 检查字符串是否为空（包括空白字符）
func IsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

// IsNotEmpty 检查字符串是否非空
func IsNotEmpty(s string) bool {
	return !IsEmpty(s)
}

// DefaultIfEmpty 如果字符串为空，返回默认值
func DefaultIfEmpty(s, defaultValue string) string {
	if IsEmpty(s) {
		return defaultValue
	}
	return s
}

// JoinNotEmpty 连接非空字符串，使用指定分隔符
func JoinNotEmpty(separator string, strs ...string) string {
	nonEmpty := make([]string, 0, len(strs))
	for _, s := range strs {
		if IsNotEmpty(s) {
			nonEmpty = append(nonEmpty, s)
		}
	}
	return strings.Join(nonEmpty, separator)
}

// FilterEmpty 过滤掉空字符串
func FilterEmpty(strs []string) []string {
	result := make([]string, 0, len(strs))
	for _, s := range strs {
		if IsNotEmpty(s) {
			result = append(result, s)
		}
	}
	return result
}

// FilterExclude 过滤掉空字符串和指定字符串
func FilterExclude(strs []string, exclude string) []string {
	result := make([]string, 0, len(strs))
	for _, s := range strs {
		if IsNotEmpty(s) && s != exclude {
			result = append(result, s)
		}
	}
	return result
}

// ContainsAny 检查字符串是否包含任意一个子字符串
func ContainsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// FormatKey 格式化 Redis key，使用冒号分隔
func FormatKey(parts ...string) string {
	return JoinNotEmpty(":", parts...)
}
