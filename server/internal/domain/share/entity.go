package share

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/pkg/utils"
)

// ShareViewMeta 分享视图元数据
type ShareViewMeta struct {
	AllowCopy          *bool              `json:"allow_copy,omitempty"`
	IncludeHiddenField *bool              `json:"include_hidden_field,omitempty"`
	Password           *SharePassword     `json:"password,omitempty"`
	IncludeRecords     *bool              `json:"include_records,omitempty"`
	Submit             *ShareSubmitConfig `json:"submit,omitempty"`
}

// SharePassword 分享密码配置
type SharePassword struct {
	Enabled bool   `json:"enabled"`
	Value   string `json:"value,omitempty"`
}

// ShareSubmitConfig 分享提交配置
type ShareSubmitConfig struct {
	Allow        *bool `json:"allow,omitempty"`
	RequireLogin *bool `json:"require_login,omitempty"`
}

// ShareView 分享视图实体
type ShareView struct {
	ID          string         `json:"id"`
	ViewID      string         `json:"view_id"`
	TableID     string         `json:"table_id"`
	ShareID     string         `json:"share_id"`
	EnableShare bool           `json:"enable_share"`
	ShareMeta   *ShareViewMeta `json:"share_meta,omitempty"`
	CreatedBy   string         `json:"created_by"`
	CreatedTime time.Time      `json:"created_time"`
	UpdatedTime time.Time      `json:"updated_time"`
}

// NewShareView 创建新的分享视图
func NewShareView(viewID, tableID, createdBy string) *ShareView {
	return &ShareView{
		ID:          utils.GenerateNanoID(10),
		ViewID:      viewID,
		TableID:     tableID,
		ShareID:     utils.GenerateNanoID(10),
		EnableShare: false,
		ShareMeta:   nil,
		CreatedBy:   createdBy,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}
}

// Enable 启用分享
func (s *ShareView) Enable(meta *ShareViewMeta) {
	s.EnableShare = true
	s.ShareMeta = meta
	s.UpdatedTime = time.Now()
}

// Disable 禁用分享
func (s *ShareView) Disable() {
	s.EnableShare = false
	s.ShareMeta = nil
	s.UpdatedTime = time.Now()
}

// UpdateMeta 更新分享元数据
func (s *ShareView) UpdateMeta(meta *ShareViewMeta) {
	s.ShareMeta = meta
	s.UpdatedTime = time.Now()
}

// IsPasswordProtected 检查是否需要密码保护
func (s *ShareView) IsPasswordProtected() bool {
	return s.ShareMeta != nil && s.ShareMeta.Password != nil && s.ShareMeta.Password.Enabled
}

// ValidatePassword 验证密码
func (s *ShareView) ValidatePassword(password string) bool {
	if !s.IsPasswordProtected() {
		return true
	}
	return s.ShareMeta.Password.Value == password
}

// AllowCopy 检查是否允许复制
func (s *ShareView) AllowCopy() bool {
	if s.ShareMeta == nil || s.ShareMeta.AllowCopy == nil {
		return false
	}
	return *s.ShareMeta.AllowCopy
}

// IncludeHiddenField 检查是否包含隐藏字段
func (s *ShareView) IncludeHiddenField() bool {
	if s.ShareMeta == nil || s.ShareMeta.IncludeHiddenField == nil {
		return false
	}
	return *s.ShareMeta.IncludeHiddenField
}

// IncludeRecords 检查是否包含记录
func (s *ShareView) IncludeRecords() bool {
	if s.ShareMeta == nil || s.ShareMeta.IncludeRecords == nil {
		return true
	}
	return *s.ShareMeta.IncludeRecords
}

// AllowSubmit 检查是否允许提交
func (s *ShareView) AllowSubmit() bool {
	if s.ShareMeta == nil || s.ShareMeta.Submit == nil || s.ShareMeta.Submit.Allow == nil {
		return false
	}
	return *s.ShareMeta.Submit.Allow
}

// RequireLoginForSubmit 检查提交是否需要登录
func (s *ShareView) RequireLoginForSubmit() bool {
	if s.ShareMeta == nil || s.ShareMeta.Submit == nil || s.ShareMeta.Submit.RequireLogin == nil {
		return false
	}
	return *s.ShareMeta.Submit.RequireLogin
}

// ShareViewInfo 分享视图信息
type ShareViewInfo struct {
	ShareView *ShareView    `json:"share_view"`
	View      interface{}   `json:"view"`   // 视图数据
	Table     interface{}   `json:"table"`  // 表格数据
	Fields    []interface{} `json:"fields"` // 字段数据
}

// ShareAuthRequest 分享认证请求
type ShareAuthRequest struct {
	ShareID  string `json:"share_id" binding:"required"`
	Password string `json:"password,omitempty"`
}

// ShareAuthResponse 分享认证响应
type ShareAuthResponse struct {
	Token   string `json:"token"`
	Expires int64  `json:"expires"`
}

// ShareFormSubmitRequest 分享表单提交请求
type ShareFormSubmitRequest struct {
	Fields   map[string]interface{} `json:"fields" binding:"required"`
	Typecast bool                   `json:"typecast,omitempty"`
}

// ShareFormSubmitResponse 分享表单提交响应
type ShareFormSubmitResponse struct {
	RecordID string                 `json:"record_id"`
	Fields   map[string]interface{} `json:"fields"`
}

// ShareCopyRequest 分享复制请求
type ShareCopyRequest struct {
	Ranges []string `json:"ranges" binding:"required"`
}

// ShareCopyResponse 分享复制响应
type ShareCopyResponse struct {
	Data string `json:"data"`
}

// ShareCollaboratorsRequest 分享协作者请求
type ShareCollaboratorsRequest struct {
	ViewID string `form:"view_id" binding:"required"`
}

// ShareCollaboratorsResponse 分享协作者响应
type ShareCollaboratorsResponse struct {
	Collaborators []interface{} `json:"collaborators"`
}

// ShareLinkRecordsRequest 分享链接记录请求
type ShareLinkRecordsRequest struct {
	FieldID string `form:"field_id" binding:"required"`
	Type    string `form:"type" binding:"required"` // candidate, selected
	Search  string `form:"search,omitempty"`
}

// ShareLinkRecordsResponse 分享链接记录响应
type ShareLinkRecordsResponse struct {
	Records []interface{} `json:"records"`
}

// ShareStats 分享统计信息
type ShareStats struct {
	TotalShares       int64     `json:"total_shares"`
	ActiveShares      int64     `json:"active_shares"`
	PasswordProtected int64     `json:"password_protected"`
	LastAccessed      time.Time `json:"last_accessed"`
}
