package commands

import (
	"context"
	"fmt"

	"github.com/easyspace-ai/luckdb/server/internal/domain/record/valueobject"
)

// Command 命令接口
type Command interface {
	CommandType() string
	Validate() error
}

// CommandHandler 命令处理器接口
type CommandHandler interface {
	Handle(ctx context.Context, cmd Command) (interface{}, error)
	CommandType() string
}

// CreateRecordCommand 创建记录命令
type CreateRecordCommand struct {
	TableID string                 `json:"table_id" validate:"required"`
	Data    map[string]interface{} `json:"data" validate:"required"`
	UserID  string                 `json:"user_id" validate:"required"`
}

func (c *CreateRecordCommand) CommandType() string {
	return "create_record"
}

func (c *CreateRecordCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if c.Data == nil {
		return fmt.Errorf("data is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// UpdateRecordCommand 更新记录命令
type UpdateRecordCommand struct {
	TableID  string                 `json:"table_id" validate:"required"`
	RecordID valueobject.RecordID   `json:"record_id" validate:"required"`
	Data     map[string]interface{} `json:"data" validate:"required"`
	UserID   string                 `json:"user_id" validate:"required"`
}

func (c *UpdateRecordCommand) CommandType() string {
	return "update_record"
}

func (c *UpdateRecordCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if c.RecordID.String() == "" {
		return fmt.Errorf("record_id is required")
	}
	if c.Data == nil {
		return fmt.Errorf("data is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// DeleteRecordCommand 删除记录命令
type DeleteRecordCommand struct {
	TableID  string               `json:"table_id" validate:"required"`
	RecordID valueobject.RecordID `json:"record_id" validate:"required"`
	UserID   string               `json:"user_id" validate:"required"`
}

func (c *DeleteRecordCommand) CommandType() string {
	return "delete_record"
}

func (c *DeleteRecordCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if c.RecordID.String() == "" {
		return fmt.Errorf("record_id is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// BatchCreateRecordsCommand 批量创建记录命令
type BatchCreateRecordsCommand struct {
	TableID string                   `json:"table_id" validate:"required"`
	Records []map[string]interface{} `json:"records" validate:"required"`
	UserID  string                   `json:"user_id" validate:"required"`
}

func (c *BatchCreateRecordsCommand) CommandType() string {
	return "batch_create_records"
}

func (c *BatchCreateRecordsCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if len(c.Records) == 0 {
		return fmt.Errorf("records is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// BatchUpdateRecordsCommand 批量更新记录命令
type BatchUpdateRecordsCommand struct {
	TableID string                            `json:"table_id" validate:"required"`
	Records map[string]map[string]interface{} `json:"records" validate:"required"`
	UserID  string                            `json:"user_id" validate:"required"`
}

func (c *BatchUpdateRecordsCommand) CommandType() string {
	return "batch_update_records"
}

func (c *BatchUpdateRecordsCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if len(c.Records) == 0 {
		return fmt.Errorf("records is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// BatchDeleteRecordsCommand 批量删除记录命令
type BatchDeleteRecordsCommand struct {
	TableID   string                 `json:"table_id" validate:"required"`
	RecordIDs []valueobject.RecordID `json:"record_ids" validate:"required"`
	UserID    string                 `json:"user_id" validate:"required"`
}

func (c *BatchDeleteRecordsCommand) CommandType() string {
	return "batch_delete_records"
}

func (c *BatchDeleteRecordsCommand) Validate() error {
	if c.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if len(c.RecordIDs) == 0 {
		return fmt.Errorf("record_ids is required")
	}
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	return nil
}
