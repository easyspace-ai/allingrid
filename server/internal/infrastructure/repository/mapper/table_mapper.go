package mapper

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/table/entity"
	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
	"github.com/easyspace-ai/luckdb/server/internal/infrastructure/database/models"
)

// ToTableEntity 将数据库模型转换为领域实体
func ToTableEntity(dbTable *models.Table) (*entity.Table, error) {
	if dbTable == nil {
		return nil, nil
	}

	// 创建 TableID
	tableID := valueobject.NewTableID(dbTable.ID)

	// 创建 TableName
	tableName, err := valueobject.NewTableName(dbTable.Name)
	if err != nil {
		return nil, err
	}

	// 处理 LastModifiedTime
	updatedAt := dbTable.CreatedTime
	if dbTable.LastModifiedTime != nil {
		updatedAt = *dbTable.LastModifiedTime
	}

	// 处理 DeletedTime
	var deletedAt *time.Time
	if dbTable.DeletedTime.Valid {
		deletedAt = &dbTable.DeletedTime.Time
	}

	// 重建实体
	table := entity.ReconstructTable(
		tableID,
		dbTable.BaseID,
		tableName,
		dbTable.Description,
		dbTable.Icon,
		dbTable.DBTableName, // ✅ 新增：物理表名
		dbTable.CreatedBy,
		dbTable.CreatedTime,
		updatedAt,
		deletedAt,
		1, // version
	)

	return table, nil
}

// ToTableModel 将领域实体转换为数据库模型
func ToTableModel(table *entity.Table) *models.Table {
	if table == nil {
		return nil
	}

	updatedAt := table.UpdatedAt()

	// ✅ 使用实体中的 dbTableName（完整路径格式："baseID"."tableID"）
	// 而不是重新生成，这样保留了 TableService 设置的完整路径
	dbTableName := table.DBTableName()

	// 设置默认Order值（数据库要求非空）
	defaultOrder := 0.0

	return &models.Table{
		ID:               table.ID().String(),
		BaseID:           table.BaseID(),
		Name:             table.Name().String(),
		DBTableName:      dbTableName, // ✅ 直接使用实体的值（包含 baseID.tableID 格式）
		Description:      table.Description(),
		Icon:             table.Icon(),
		CreatedBy:        table.CreatedBy(),
		CreatedTime:      table.CreatedAt(),
		LastModifiedTime: &updatedAt,
		Order:            &defaultOrder,
	}
}

// ToTableList 批量转换
func ToTableList(dbTables []*models.Table) ([]*entity.Table, error) {
	tables := make([]*entity.Table, 0, len(dbTables))
	for _, dbTable := range dbTables {
		table, err := ToTableEntity(dbTable)
		if err != nil {
			return nil, err
		}
		if table != nil {
			tables = append(tables, table)
		}
	}
	return tables, nil
}
