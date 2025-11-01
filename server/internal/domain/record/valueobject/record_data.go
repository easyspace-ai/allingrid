package valueobject

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record"
)

// RecordData 记录数据值对象
// 封装动态的字段值数据
type RecordData struct {
	values map[string]interface{} // 字段名 -> 字段值
	hash   string                 // 数据哈希值（用于变更检测）
}

// NewRecordData 创建记录数据
func NewRecordData(values map[string]interface{}) (RecordData, error) {
	if values == nil {
		values = make(map[string]interface{})
	}

	// 计算哈希
	hash, err := calculateHash(values)
	if err != nil {
		return RecordData{}, err
	}

	return RecordData{
		values: values,
		hash:   hash,
	}, nil
}

// Get 获取字段值
func (rd RecordData) Get(fieldName string) (interface{}, bool) {
	value, exists := rd.values[fieldName]
	return value, exists
}

// GetString 获取字符串类型的字段值
func (rd RecordData) GetString(fieldName string) (string, bool) {
	value, exists := rd.values[fieldName]
	if !exists {
		return "", false
	}

	str, ok := value.(string)
	return str, ok
}

// GetInt 获取整数类型的字段值
func (rd RecordData) GetInt(fieldName string) (int64, bool) {
	value, exists := rd.values[fieldName]
	if !exists {
		return 0, false
	}

	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// GetFloat 获取浮点数类型的字段值
func (rd RecordData) GetFloat(fieldName string) (float64, bool) {
	value, exists := rd.values[fieldName]
	if !exists {
		return 0, false
	}

	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// Set 设置字段值（返回新的RecordData，保持不可变性）
func (rd RecordData) Set(fieldName string, value interface{}) (RecordData, error) {
	// 创建新的值映射
	newValues := make(map[string]interface{})
	for k, v := range rd.values {
		newValues[k] = v
	}
	newValues[fieldName] = value

	return NewRecordData(newValues)
}

// Delete 删除字段值（返回新的RecordData）
func (rd RecordData) Delete(fieldName string) (RecordData, error) {
	// 创建新的值映射
	newValues := make(map[string]interface{})
	for k, v := range rd.values {
		if k != fieldName {
			newValues[k] = v
		}
	}

	return NewRecordData(newValues)
}

// Merge 合并数据（返回新的RecordData）
func (rd RecordData) Merge(other RecordData) (RecordData, error) {
	newValues := make(map[string]interface{})

	// 复制当前值
	for k, v := range rd.values {
		newValues[k] = v
	}

	// 合并新值
	for k, v := range other.values {
		newValues[k] = v
	}

	return NewRecordData(newValues)
}

// HasField 检查是否包含指定字段
func (rd RecordData) HasField(fieldName string) bool {
	_, exists := rd.values[fieldName]
	return exists
}

// FieldCount 获取字段数量
func (rd RecordData) FieldCount() int {
	return len(rd.values)
}

// IsEmpty 检查数据是否为空
func (rd RecordData) IsEmpty() bool {
	return len(rd.values) == 0
}

// Hash 获取数据哈希值
func (rd RecordData) Hash() string {
	return rd.hash
}

// HasChanged 检查数据是否发生变更
func (rd RecordData) HasChanged(other RecordData) bool {
	return rd.hash != other.hash
}

// ToMap 转换为map（返回副本）
func (rd RecordData) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range rd.values {
		result[k] = v
	}
	return result
}

// calculateHash 计算数据哈希
func calculateHash(values map[string]interface{}) (string, error) {
	// 序列化为JSON
	data, err := json.Marshal(values)
	if err != nil {
		return "", record.NewDomainError(
			"HASH_CALCULATION_FAILED",
			"failed to calculate data hash",
			err,
		)
	}

	// 计算SHA256哈希
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}
