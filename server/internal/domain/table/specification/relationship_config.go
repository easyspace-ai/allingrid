package specification

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/table/valueobject"
)

// RelationshipConfig 关系配置
// 注意：这不是值对象，而是关系的配置实体
type RelationshipConfig struct {
	ID                  string
	SourceTableID       string
	SourceFieldID       string
	TargetTableID       string
	TargetFieldID       string
	RelationType        valueobject.RelationType
	DisplayField        string
	AllowLinkToMultiple bool
	IsSymmetric         bool
	CascadeDelete       bool
	OnDeleteAction      string // cascade, set_null, restrict
	OnUpdateAction      string // cascade, set_null, restrict
	CreatedBy           string
	CreatedTime         time.Time
	LastModifiedTime    *time.Time
}

// RelationshipConstraint 关系约束
type RelationshipConstraint struct {
	Type         string // unique, foreign_key, check
	Expression   string
	ErrorMessage string
	IsActive     bool
}

// RelationshipImpactAnalysis 关系影响分析
type RelationshipImpactAnalysis struct {
	AffectedTables     []string
	AffectedRecords    int64
	AffectedFields     []string
	BreakingChanges    []string
	Warnings           []string
	RequiredMigrations []string
}

// RelationshipSpecification 关系规约接口
type RelationshipSpecification interface {
	// IsSatisfiedBy 检查关系配置是否满足规约
	IsSatisfiedBy(config *RelationshipConfig) bool

	// GetErrorMessage 获取不满足规约时的错误信息
	GetErrorMessage() string
}

// ValidRelationTypeSpec 有效关系类型规约
type ValidRelationTypeSpec struct{}

func (s *ValidRelationTypeSpec) IsSatisfiedBy(config *RelationshipConfig) bool {
	return config.RelationType.IsValid()
}

func (s *ValidRelationTypeSpec) GetErrorMessage() string {
	return "invalid relation type"
}

// SymmetricFieldRequiredSpec 对称字段必需规约
type SymmetricFieldRequiredSpec struct{}

func (s *SymmetricFieldRequiredSpec) IsSatisfiedBy(config *RelationshipConfig) bool {
	// 如果是对称关系，必须有目标字段ID
	if config.IsSymmetric {
		return config.TargetFieldID != ""
	}
	return true
}

func (s *SymmetricFieldRequiredSpec) GetErrorMessage() string {
	return "symmetric relationships require target field ID"
}

// ValidateRelationshipConfig 验证关系配置
func ValidateRelationshipConfig(config *RelationshipConfig) error {
	specs := []RelationshipSpecification{
		&ValidRelationTypeSpec{},
		&SymmetricFieldRequiredSpec{},
	}

	for _, spec := range specs {
		if !spec.IsSatisfiedBy(config) {
			return &ValidationError{Message: spec.GetErrorMessage()}
		}
	}

	return nil
}

// ValidationError 验证错误
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
