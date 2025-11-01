package valueobject

// RelationType 关系类型枚举
type RelationType string

const (
	// OneToMany 一对多关系
	OneToMany RelationType = "oneToMany"
	// ManyToOne 多对一关系
	ManyToOne RelationType = "manyToOne"
	// ManyToMany 多对多关系
	ManyToMany RelationType = "manyToMany"
	// OneToOne 一对一关系
	OneToOne RelationType = "oneToOne"
)

// IsValid 验证关系类型是否有效
func (rt RelationType) IsValid() bool {
	switch rt {
	case OneToMany, ManyToOne, ManyToMany, OneToOne:
		return true
	default:
		return false
	}
}

// String 返回关系类型的字符串表示
func (rt RelationType) String() string {
	return string(rt)
}

// IsSymmetric 判断关系是否需要对称同步
func (rt RelationType) IsSymmetric() bool {
	// OneToOne 和 ManyToMany 需要对称同步
	return rt == OneToOne || rt == ManyToMany
}

// Relationship 关系类型（用于字段 Options，向后兼容）
type Relationship string

const (
	// HasOne 拥有一个（OneToOne）
	HasOne Relationship = "hasOne"
	// BelongsTo 属于（ManyToOne）
	BelongsTo Relationship = "belongsTo"
	// HasMany 拥有多个（OneToMany）
	HasMany Relationship = "hasMany"
	// ManyToManyRelation 多对多
	ManyToManyRelation Relationship = "manyToMany"
)

// ToRelationType 转换为 RelationType
func (r Relationship) ToRelationType() RelationType {
	switch r {
	case HasOne:
		return OneToOne
	case BelongsTo:
		return ManyToOne
	case HasMany:
		return OneToMany
	case ManyToManyRelation:
		return ManyToMany
	default:
		return ""
	}
}

// IsValid 验证关系是否有效
func (r Relationship) IsValid() bool {
	switch r {
	case HasOne, BelongsTo, HasMany, ManyToManyRelation:
		return true
	default:
		return false
	}
}
