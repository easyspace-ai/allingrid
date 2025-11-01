package entity

import (
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/view/valueobject"
	"github.com/easyspace-ai/luckdb/server/pkg/utils"

	"github.com/google/uuid"
)

// View 视图实体
type View struct {
	// 标识
	id string

	// 基本信息
	name        string
	description string
	tableID     string

	// 视图配置
	viewType   valueobject.ViewType
	filter     *valueobject.Filter
	sort       *valueobject.Sort
	group      *valueobject.Group
	columnMeta *valueobject.ColumnMetaList
	options    map[string]interface{} // 视图特定选项

	// 排序和版本
	order   float64
	version int

	// 锁定状态
	isLocked bool

	// 分享设置
	enableShare bool
	shareID     *string
	shareMeta   map[string]interface{}

	// 审计信息
	createdBy string
	createdAt time.Time
	updatedAt time.Time
	deletedAt *time.Time
}

// NewView 创建新视图
func NewView(
	tableID string,
	name string,
	viewType valueobject.ViewType,
	createdBy string,
) (*View, error) {
	// 验证
	if tableID == "" {
		return nil, fmt.Errorf("table ID is required")
	}
	if name == "" {
		return nil, fmt.Errorf("view name is required")
	}
	if !viewType.IsValid() {
		return nil, fmt.Errorf("invalid view type: %s", viewType)
	}
	if createdBy == "" {
		return nil, fmt.Errorf("created by is required")
	}

	now := time.Now()

	return &View{
		id:          utils.GenerateViewID(), // 使用带前缀的短ID而不是UUID
		tableID:     tableID,
		name:        name,
		description: "",
		viewType:    viewType,
		filter:      nil,
		sort:        nil,
		group:       nil,
		columnMeta:  &valueobject.ColumnMetaList{Columns: []valueobject.ColumnMeta{}},
		options:     make(map[string]interface{}),
		order:       0.0, // 默认顺序为0，实际顺序应该由ViewService根据当前视图数量计算
		version:     1,
		isLocked:    false,
		enableShare: false,
		shareID:     nil,
		shareMeta:   nil,
		createdBy:   createdBy,
		createdAt:   now,
		updatedAt:   now,
		deletedAt:   nil,
	}, nil
}

// ReconstructView 重建视图实体（用于从数据库加载）
func ReconstructView(
	id string,
	name string,
	description string,
	tableID string,
	viewType valueobject.ViewType,
	filter *valueobject.Filter,
	sort *valueobject.Sort,
	group *valueobject.Group,
	columnMeta *valueobject.ColumnMetaList,
	options map[string]interface{},
	order float64,
	version int,
	isLocked bool,
	enableShare bool,
	shareID *string,
	shareMeta map[string]interface{},
	createdBy string,
	createdAt time.Time,
	updatedAt time.Time,
	deletedAt *time.Time,
) *View {
	return &View{
		id:          id,
		name:        name,
		description: description,
		tableID:     tableID,
		viewType:    viewType,
		filter:      filter,
		sort:        sort,
		group:       group,
		columnMeta:  columnMeta,
		options:     options,
		order:       order,
		version:     version,
		isLocked:    isLocked,
		enableShare: enableShare,
		shareID:     shareID,
		shareMeta:   shareMeta,
		createdBy:   createdBy,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
		deletedAt:   deletedAt,
	}
}

// Getter 方法

func (v *View) ID() string                              { return v.id }
func (v *View) Name() string                            { return v.name }
func (v *View) Description() string                     { return v.description }
func (v *View) TableID() string                         { return v.tableID }
func (v *View) ViewType() valueobject.ViewType          { return v.viewType }
func (v *View) Filter() *valueobject.Filter             { return v.filter }
func (v *View) Sort() *valueobject.Sort                 { return v.sort }
func (v *View) Group() *valueobject.Group               { return v.group }
func (v *View) ColumnMeta() *valueobject.ColumnMetaList { return v.columnMeta }
func (v *View) Options() map[string]interface{}         { return v.options }
func (v *View) Order() float64                          { return v.order }
func (v *View) Version() int                            { return v.version }
func (v *View) IsLocked() bool                          { return v.isLocked }
func (v *View) EnableShare() bool                       { return v.enableShare }
func (v *View) ShareID() *string                        { return v.shareID }
func (v *View) ShareMeta() map[string]interface{}       { return v.shareMeta }
func (v *View) CreatedBy() string                       { return v.createdBy }
func (v *View) CreatedAt() time.Time                    { return v.createdAt }
func (v *View) UpdatedAt() time.Time                    { return v.updatedAt }
func (v *View) DeletedAt() *time.Time                   { return v.deletedAt }

// 业务方法

// UpdateName 更新视图名称
func (v *View) UpdateName(name string) error {
	if name == "" {
		return fmt.Errorf("view name cannot be empty")
	}

	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	v.name = name
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateDescription 更新描述
func (v *View) UpdateDescription(description string) {
	v.description = description
	v.updatedAt = time.Now()
	v.version++
}

// UpdateFilter 更新过滤器
func (v *View) UpdateFilter(filter *valueobject.Filter) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if filter != nil {
		if err := filter.Validate(); err != nil {
			return fmt.Errorf("invalid filter: %w", err)
		}
	}

	v.filter = filter
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateSort 更新排序
func (v *View) UpdateSort(sort *valueobject.Sort) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if !v.viewType.SupportsSort() {
		return fmt.Errorf("view type %s does not support sorting", v.viewType)
	}

	if sort != nil {
		if err := sort.Validate(); err != nil {
			return fmt.Errorf("invalid sort: %w", err)
		}
	}

	v.sort = sort
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateGroup 更新分组
func (v *View) UpdateGroup(group *valueobject.Group) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if !v.viewType.SupportsGroup() {
		return fmt.Errorf("view type %s does not support grouping", v.viewType)
	}

	if group != nil {
		if err := group.Validate(); err != nil {
			return fmt.Errorf("invalid group: %w", err)
		}
	}

	v.group = group
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateColumnMeta 更新列配置
func (v *View) UpdateColumnMeta(columnMeta *valueobject.ColumnMetaList) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if columnMeta != nil {
		if err := columnMeta.Validate(); err != nil {
			return fmt.Errorf("invalid column meta: %w", err)
		}
	}

	v.columnMeta = columnMeta
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateOptions 更新选项
func (v *View) UpdateOptions(options map[string]interface{}) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if options == nil {
		options = make(map[string]interface{})
	}

	v.options = options
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// PatchOptions 部分更新选项
func (v *View) PatchOptions(options map[string]interface{}) error {
	if v.isLocked {
		return fmt.Errorf("cannot update locked view")
	}

	if v.options == nil {
		v.options = make(map[string]interface{})
	}

	for key, value := range options {
		v.options[key] = value
	}

	v.updatedAt = time.Now()
	v.version++

	return nil
}

// UpdateOrder 更新排序位置
func (v *View) UpdateOrder(order float64) error {
	if order < 0 {
		return fmt.Errorf("order must be non-negative")
	}

	v.order = order
	v.updatedAt = time.Now()

	return nil
}

// Lock 锁定视图
func (v *View) Lock() {
	v.isLocked = true
	v.updatedAt = time.Now()
	v.version++
}

// Unlock 解锁视图
func (v *View) Unlock() {
	v.isLocked = false
	v.updatedAt = time.Now()
	v.version++
}

// EnableSharing 启用分享
func (v *View) EnableSharing() (string, error) {
	if v.shareID != nil && v.enableShare {
		// 已经启用分享，返回现有的shareID
		return *v.shareID, nil
	}

	// 生成新的shareID
	shareID := uuid.New().String()
	v.shareID = &shareID
	v.enableShare = true
	v.updatedAt = time.Now()
	v.version++

	return shareID, nil
}

// DisableSharing 禁用分享
func (v *View) DisableSharing() {
	v.enableShare = false
	v.shareID = nil
	v.shareMeta = nil
	v.updatedAt = time.Now()
	v.version++
}

// RefreshShareID 刷新分享ID
func (v *View) RefreshShareID() (string, error) {
	if !v.enableShare {
		return "", fmt.Errorf("sharing is not enabled")
	}

	shareID := uuid.New().String()
	v.shareID = &shareID
	v.updatedAt = time.Now()
	v.version++

	return shareID, nil
}

// UpdateShareMeta 更新分享元数据
func (v *View) UpdateShareMeta(shareMeta map[string]interface{}) error {
	if !v.enableShare {
		return fmt.Errorf("sharing is not enabled")
	}

	v.shareMeta = shareMeta
	v.updatedAt = time.Now()
	v.version++

	return nil
}

// Delete 软删除视图
func (v *View) Delete() {
	now := time.Now()
	v.deletedAt = &now
	v.updatedAt = now
}

// IsDeleted 检查是否已删除
func (v *View) IsDeleted() bool {
	return v.deletedAt != nil
}

// CanEdit 检查是否可以编辑
func (v *View) CanEdit() bool {
	return !v.isLocked && !v.IsDeleted()
}

// GetAllFieldIDs 获取所有涉及的字段ID
func (v *View) GetAllFieldIDs() []string {
	fieldIDMap := make(map[string]bool)

	// 从列配置获取
	if v.columnMeta != nil {
		for _, fieldID := range v.columnMeta.GetFieldIDs() {
			fieldIDMap[fieldID] = true
		}
	}

	// 从过滤器获取
	if v.filter != nil {
		for _, fieldID := range v.filter.GetFieldIDs() {
			fieldIDMap[fieldID] = true
		}
	}

	// 从排序获取
	if v.sort != nil {
		for _, fieldID := range v.sort.GetFieldIDs() {
			fieldIDMap[fieldID] = true
		}
	}

	// 从分组获取
	if v.group != nil {
		for _, fieldID := range v.group.GetFieldIDs() {
			fieldIDMap[fieldID] = true
		}
	}

	// 转换为切片
	fieldIDs := make([]string, 0, len(fieldIDMap))
	for fieldID := range fieldIDMap {
		fieldIDs = append(fieldIDs, fieldID)
	}

	return fieldIDs
}

// Clone 克隆视图（用于复制）
func (v *View) Clone(newName string, createdBy string) (*View, error) {
	return &View{
		id:          uuid.New().String(),
		name:        newName,
		description: v.description,
		tableID:     v.tableID,
		viewType:    v.viewType,
		filter:      v.filter,     // 浅拷贝，如需深拷贝需额外处理
		sort:        v.sort,       // 浅拷贝
		group:       v.group,      // 浅拷贝
		columnMeta:  v.columnMeta, // 浅拷贝
		options:     v.options,    // 浅拷贝
		order:       float64(time.Now().UnixNano()),
		version:     1,
		isLocked:    false,
		enableShare: false,
		shareID:     nil,
		shareMeta:   nil,
		createdBy:   createdBy,
		createdAt:   time.Now(),
		updatedAt:   time.Now(),
		deletedAt:   nil,
	}, nil
}
