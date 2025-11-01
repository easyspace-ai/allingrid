package attachment

import "context"

// Repository 附件仓储接口
type Repository interface {
	// CreateAttachment 创建附件
	CreateAttachment(ctx context.Context, attachment *AttachmentItem) error
	// GetAttachmentByID 通过ID获取附件
	GetAttachmentByID(ctx context.Context, id string) (*AttachmentItem, error)
	// GetAttachmentByToken 通过令牌获取附件
	GetAttachmentByToken(ctx context.Context, token string) (*AttachmentItem, error)
	// GetAttachmentByPath 通过路径获取附件
	GetAttachmentByPath(ctx context.Context, path string) (*AttachmentItem, error)
	// UpdateAttachment 更新附件
	UpdateAttachment(ctx context.Context, attachment *AttachmentItem) error
	// DeleteAttachment 删除附件
	DeleteAttachment(ctx context.Context, id string) error
	// ListAttachments 列出附件
	ListAttachments(ctx context.Context, tableID, fieldID, recordID string) ([]*AttachmentItem, error)
	// GetAttachmentStats 获取附件统计信息
	GetAttachmentStats(ctx context.Context, tableID string) (*AttachmentStats, error)
}

// UploadTokenRepository 上传令牌仓储接口
type UploadTokenRepository interface {
	// CreateUploadToken 创建上传令牌
	CreateUploadToken(ctx context.Context, token *UploadToken) error
	// GetUploadToken 获取上传令牌
	GetUploadToken(ctx context.Context, token string) (*UploadToken, error)
	// DeleteUploadToken 删除上传令牌
	DeleteUploadToken(ctx context.Context, token string) error
	// CleanupExpiredTokens 清理过期令牌
	CleanupExpiredTokens(ctx context.Context) error
}
