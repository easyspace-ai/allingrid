package helpers

import (
	"github.com/easyspace-ai/luckdb/server/internal/application/constants"
	"github.com/easyspace-ai/luckdb/server/internal/application/dto"
)

// PrepareTableDefaults 准备表格默认值
// 参考 Teable 的 TablePipe.prepareDefaultRo
// 如果请求中没有提供 views 或 fields，则注入默认值
func PrepareTableDefaults(req *dto.CreateTableRequest) {
	// 注入默认视图配置
	if len(req.Views) == 0 {
		defaultViews := constants.GetDefaultViews()
		req.Views = make([]dto.ViewConfigDTO, len(defaultViews))
		for i, v := range defaultViews {
			req.Views[i] = dto.ViewConfigDTO{
				Name:        v.Name,
				Type:        v.Type,
				Description: v.Description,
				ColumnMeta:  v.ColumnMeta,
			}
		}
	}

	// 注入默认字段配置
	if len(req.Fields) == 0 {
		defaultFields := constants.GetDefaultFields()
		req.Fields = make([]dto.FieldConfigDTO, len(defaultFields))
		for i, f := range defaultFields {
			req.Fields[i] = dto.FieldConfigDTO{
				Name:        f.Name,
				Type:        f.Type,
				Description: f.Description,
				Required:    f.Required,
				Unique:      f.Unique,
				IsPrimary:   f.IsPrimary,
				Options:     f.Options,
			}
		}
	}

	// 确保第一个字段是主字段（参考 Teable）
	if len(req.Fields) > 0 && !hasAnyPrimaryField(req.Fields) {
		req.Fields[0].IsPrimary = true
	}
}

// hasAnyPrimaryField 检查是否已有主字段
func hasAnyPrimaryField(fields []dto.FieldConfigDTO) bool {
	for _, f := range fields {
		if f.IsPrimary {
			return true
		}
	}
	return false
}
