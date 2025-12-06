// Package array 提供数组和切片处理的工具函数
//
// Author: Ruan Huipeng
// Date: 2025-12-01

package array

// Contains 检查切片中是否包含指定元素
func Contains[T comparable](slice []T, item T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// ContainsFunc 使用函数检查切片中是否包含满足条件的元素
func ContainsFunc[T any](slice []T, fn func(T) bool) bool {
	for _, v := range slice {
		if fn(v) {
			return true
		}
	}
	return false
}

// Filter 过滤切片，返回满足条件的元素
func Filter[T any](slice []T, fn func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for _, v := range slice {
		if fn(v) {
			result = append(result, v)
		}
	}
	return result
}

// Map 对切片中的每个元素进行转换
func Map[T any, R any](slice []T, fn func(T) R) []R {
	result := make([]R, len(slice))
	for i, v := range slice {
		result[i] = fn(v)
	}
	return result
}

// Distinct 去重，返回不重复的元素
func Distinct[T comparable](slice []T) []T {
	seen := make(map[T]struct{})
	result := make([]T, 0, len(slice))
	for _, v := range slice {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// IsEmpty 检查切片是否为空
func IsEmpty[T any](slice []T) bool {
	return len(slice) == 0
}

// IsNotEmpty 检查切片是否非空
func IsNotEmpty[T any](slice []T) bool {
	return !IsEmpty(slice)
}

// First 返回切片的第一个元素和是否存在的标志
func First[T any](slice []T) (T, bool) {
	if len(slice) == 0 {
		var zero T
		return zero, false
	}
	return slice[0], true
}

// Last 返回切片的最后一个元素和是否存在的标志
func Last[T any](slice []T) (T, bool) {
	if len(slice) == 0 {
		var zero T
		return zero, false
	}
	return slice[len(slice)-1], true
}

