package models

import (
	"encoding/json"
	"time"

	"gorm.io/gorm"
)

// Record 记录模型
type Record struct {
	ID               string         `gorm:"primaryKey;type:varchar(50)" json:"id"`
	TableID          string         `gorm:"type:varchar(50);not null;index" json:"table_id"`
	Data             string         `gorm:"type:jsonb" json:"data"` // 存储为JSON格式
	CreatedBy        string         `gorm:"type:varchar(50);not null;index" json:"created_by"`
	CreatedTime      time.Time      `gorm:"not null" json:"created_at"`
	DeletedTime      gorm.DeletedAt `gorm:"index" json:"deleted_time"`
	LastModifiedTime *time.Time     `json:"updated_at"`
}

// TableName 指定表名
func (Record) TableName() string {
	return "record"
}

// GetDataAsMap 将JSON数据转换为map
func (r *Record) GetDataAsMap() (map[string]interface{}, error) {
	var data map[string]interface{}
	if r.Data != "" {
		err := json.Unmarshal([]byte(r.Data), &data)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// SetDataFromMap 将map数据转换为JSON存储
func (r *Record) SetDataFromMap(data map[string]interface{}) error {
	if data == nil {
		r.Data = "{}"
		return nil
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	r.Data = string(jsonData)
	return nil
}
