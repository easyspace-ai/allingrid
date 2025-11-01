package service

import "context"

// RecordData 记录数据接口
// 使用接口避免循环依赖
type RecordData interface {
	GetID() string
	GetTableID() string
	GetData() map[string]interface{}
	SetData(map[string]interface{})
}

// SimpleRecord 简单记录实现（用于测试和独立使用）
type SimpleRecord struct {
	ID      string
	TableID string
	Data    map[string]interface{}
}

func (r *SimpleRecord) GetID() string {
	return r.ID
}

func (r *SimpleRecord) GetTableID() string {
	return r.TableID
}

func (r *SimpleRecord) GetData() map[string]interface{} {
	return r.Data
}

func (r *SimpleRecord) SetData(data map[string]interface{}) {
	r.Data = data
}

// RecordServiceInterface 记录服务接口
type RecordServiceInterface interface {
	GetByID(ctx context.Context, tableID, recordID string) (RecordData, error)
	GetByIDs(ctx context.Context, tableID string, recordIDs []string) ([]RecordData, error)
	GetByTableID(ctx context.Context, tableID string) ([]RecordData, error)
	Update(ctx context.Context, rec RecordData) error
}
