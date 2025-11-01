package valueobject

// LinkFieldOptions Link字段选项
type LinkFieldOptions struct {
	// RelatedTableID 关联的表ID
	RelatedTableID string `json:"relatedTableId"`

	// RelatedFieldID 对称字段ID（双向关联）
	RelatedFieldID string `json:"relatedFieldId,omitempty"`

	// RelationType 关系类型
	RelationType RelationType `json:"relationType"`

	// ForeignKeyFieldID 外键字段ID
	ForeignKeyFieldID string `json:"foreignKeyFieldId,omitempty"`

	// IsOneWay 是否为单向关联
	IsOneWay bool `json:"isOneWay,omitempty"`

	// AllowMultiple 是否允许多个关联
	AllowMultiple bool `json:"allowMultiple,omitempty"`
}

// NewLinkFieldOptions 创建Link字段选项
func NewLinkFieldOptions(relatedTableID string, relationType RelationType) (*LinkFieldOptions, error) {
	if relatedTableID == "" {
		return nil, ErrRelatedTableIDRequired
	}

	if !relationType.IsValid() {
		return nil, ErrInvalidRelationType
	}

	return &LinkFieldOptions{
		RelatedTableID: relatedTableID,
		RelationType:   relationType,
		IsOneWay:       false,
		AllowMultiple:  relationType == ManyToMany || relationType == OneToMany,
	}, nil
}

// WithSymmetricField 设置对称字段
func (o *LinkFieldOptions) WithSymmetricField(fieldID string) *LinkFieldOptions {
	o.RelatedFieldID = fieldID
	o.IsOneWay = false
	return o
}

// WithForeignKey 设置外键字段
func (o *LinkFieldOptions) WithForeignKey(fieldID string) *LinkFieldOptions {
	o.ForeignKeyFieldID = fieldID
	return o
}

// AsOneWay 设置为单向关联
func (o *LinkFieldOptions) AsOneWay() *LinkFieldOptions {
	o.IsOneWay = true
	o.RelatedFieldID = ""
	return o
}

// Validate 验证Link字段选项
func (o *LinkFieldOptions) Validate() error {
	if o.RelatedTableID == "" {
		return ErrRelatedTableIDRequired
	}

	if !o.RelationType.IsValid() {
		return ErrInvalidRelationType
	}

	// 对称关系需要对称字段ID
	if o.RelationType.IsSymmetric() && !o.IsOneWay && o.RelatedFieldID == "" {
		return ErrSymmetricFieldRequired
	}

	return nil
}

// HasSymmetricField 是否有对称字段
func (o *LinkFieldOptions) HasSymmetricField() bool {
	return !o.IsOneWay && o.RelatedFieldID != ""
}

// NeedsSymmetricSync 是否需要对称同步
func (o *LinkFieldOptions) NeedsSymmetricSync() bool {
	return o.HasSymmetricField() && o.RelationType.IsSymmetric()
}
