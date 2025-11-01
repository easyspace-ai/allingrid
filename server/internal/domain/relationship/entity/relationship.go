package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/relationship/valueobject"

	"github.com/google/uuid"
)

// Relationship 关系实体
// 表示两个表之间的关系
type Relationship struct {
	// 基础属性
	id            string
	sourceTableID string
	sourceFieldID string
	targetTableID string
	targetFieldID string
	relationType  valueobject.RelationType

	// 配置
	displayField   string
	isSymmetric    bool
	cascadeDelete  bool
	onDeleteAction string
	onUpdateAction string

	// 审计
	createdBy string
	createdAt time.Time
	updatedAt time.Time
}

// NewRelationship 创建新关系
func NewRelationship(
	sourceTableID, sourceFieldID string,
	targetTableID, targetFieldID string,
	relationType valueobject.RelationType,
	createdBy string,
) (*Relationship, error) {
	// 验证
	if sourceTableID == "" || targetTableID == "" {
		return nil, valueobject.NewDomainError(
			"INVALID_TABLE_ID",
			"source and target table IDs are required",
			nil,
		)
	}

	if sourceFieldID == "" || targetFieldID == "" {
		return nil, valueobject.NewDomainError(
			"INVALID_FIELD_ID",
			"source and target field IDs are required",
			nil,
		)
	}

	// 不能链接到同一张表（除非是特殊情况）
	if sourceTableID == targetTableID {
		return nil, valueobject.NewDomainError(
			"SELF_REFERENCE_NOT_ALLOWED",
			"cannot create relationship to the same table",
			nil,
		)
	}

	now := time.Now()

	return &Relationship{
		id:             uuid.New().String(),
		sourceTableID:  sourceTableID,
		sourceFieldID:  sourceFieldID,
		targetTableID:  targetTableID,
		targetFieldID:  targetFieldID,
		relationType:   relationType,
		isSymmetric:    false,
		cascadeDelete:  false,
		onDeleteAction: "restrict",
		onUpdateAction: "restrict",
		createdBy:      createdBy,
		createdAt:      now,
		updatedAt:      now,
	}, nil
}

// ==================== 访问器方法 ====================

func (r *Relationship) ID() string                             { return r.id }
func (r *Relationship) SourceTableID() string                  { return r.sourceTableID }
func (r *Relationship) SourceFieldID() string                  { return r.sourceFieldID }
func (r *Relationship) TargetTableID() string                  { return r.targetTableID }
func (r *Relationship) TargetFieldID() string                  { return r.targetFieldID }
func (r *Relationship) RelationType() valueobject.RelationType { return r.relationType }
func (r *Relationship) DisplayField() string                   { return r.displayField }
func (r *Relationship) IsSymmetric() bool                      { return r.isSymmetric }
func (r *Relationship) CascadeDelete() bool                    { return r.cascadeDelete }
func (r *Relationship) OnDeleteAction() string                 { return r.onDeleteAction }
func (r *Relationship) OnUpdateAction() string                 { return r.onUpdateAction }
func (r *Relationship) CreatedBy() string                      { return r.createdBy }
func (r *Relationship) CreatedAt() time.Time                   { return r.createdAt }
func (r *Relationship) UpdatedAt() time.Time                   { return r.updatedAt }

// ==================== 业务方法 ====================

// SetSymmetric 设置对称关系
func (r *Relationship) SetSymmetric(symmetric bool) {
	r.isSymmetric = symmetric
	r.updatedAt = time.Now()
}

// SetCascadeDelete 设置级联删除
func (r *Relationship) SetCascadeDelete(cascade bool) {
	r.cascadeDelete = cascade
	if cascade {
		r.onDeleteAction = "cascade"
	} else {
		r.onDeleteAction = "restrict"
	}
	r.updatedAt = time.Now()
}

// SetDisplayField 设置显示字段
func (r *Relationship) SetDisplayField(fieldName string) {
	r.displayField = fieldName
	r.updatedAt = time.Now()
}

// SetDeleteAction 设置删除动作
func (r *Relationship) SetDeleteAction(action string) error {
	validActions := map[string]bool{
		"cascade":  true,
		"set_null": true,
		"restrict": true,
	}

	if !validActions[action] {
		return valueobject.NewDomainError(
			"INVALID_DELETE_ACTION",
			"invalid delete action: must be cascade, set_null, or restrict",
			nil,
		)
	}

	r.onDeleteAction = action
	r.updatedAt = time.Now()

	return nil
}

// SetUpdateAction 设置更新动作
func (r *Relationship) SetUpdateAction(action string) error {
	validActions := map[string]bool{
		"cascade":  true,
		"set_null": true,
		"restrict": true,
	}

	if !validActions[action] {
		return valueobject.NewDomainError(
			"INVALID_UPDATE_ACTION",
			"invalid update action: must be cascade, set_null, or restrict",
			nil,
		)
	}

	r.onUpdateAction = action
	r.updatedAt = time.Now()

	return nil
}

// IsReversible 是否可逆（对称关系）
func (r *Relationship) IsReversible() bool {
	return r.isSymmetric && (r.relationType.IsOneToOne() || r.relationType.IsManyToMany())
}
