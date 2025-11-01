package valueobject

// RelationType 关系类型值对象
type RelationType struct {
	value string
}

const (
	OneToOne   = "one_to_one"
	OneToMany  = "one_to_many"
	ManyToOne  = "many_to_one"
	ManyToMany = "many_to_many"
)

// NewRelationType 创建关系类型
func NewRelationType(value string) (RelationType, error) {
	if !isValidRelationType(value) {
		return RelationType{}, ErrInvalidRelationType
	}

	return RelationType{value: value}, nil
}

// String 获取字符串值
func (rt RelationType) String() string {
	return rt.value
}

// Equals 比较两个关系类型是否相等
func (rt RelationType) Equals(other RelationType) bool {
	return rt.value == other.value
}

// IsOneToOne 是否为一对一关系
func (rt RelationType) IsOneToOne() bool {
	return rt.value == OneToOne
}

// IsOneToMany 是否为一对多关系
func (rt RelationType) IsOneToMany() bool {
	return rt.value == OneToMany
}

// IsManyToOne 是否为多对一关系
func (rt RelationType) IsManyToOne() bool {
	return rt.value == ManyToOne
}

// IsManyToMany 是否为多对多关系
func (rt RelationType) IsManyToMany() bool {
	return rt.value == ManyToMany
}

// AllowsMultiple 是否允许多个关联
func (rt RelationType) AllowsMultiple() bool {
	return rt.value == OneToMany || rt.value == ManyToMany
}

// RequiresJunctionTable 是否需要中间表
func (rt RelationType) RequiresJunctionTable() bool {
	return rt.value == ManyToMany
}

// GetInverseRelationType 获取反向关系类型
func (rt RelationType) GetInverseRelationType() RelationType {
	switch rt.value {
	case OneToOne:
		return RelationType{value: OneToOne}
	case OneToMany:
		return RelationType{value: ManyToOne}
	case ManyToOne:
		return RelationType{value: OneToMany}
	case ManyToMany:
		return RelationType{value: ManyToMany}
	default:
		return RelationType{}
	}
}

// isValidRelationType 检查关系类型是否有效
func isValidRelationType(value string) bool {
	validTypes := map[string]bool{
		OneToOne:   true,
		OneToMany:  true,
		ManyToOne:  true,
		ManyToMany: true,
	}

	return validTypes[value]
}

// ErrInvalidRelationType 无效的关系类型错误
var ErrInvalidRelationType = NewDomainError(
	"INVALID_RELATION_TYPE",
	"invalid relation type",
	nil,
)

// DomainError 领域错误
type DomainError struct {
	Code    string
	Message string
	Err     error
}

func (e *DomainError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func NewDomainError(code, message string, err error) *DomainError {
	return &DomainError{Code: code, Message: message, Err: err}
}
