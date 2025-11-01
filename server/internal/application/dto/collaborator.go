package dto

// CollaboratorResponse 协作者响应DTO
type CollaboratorResponse struct {
	ID            string `json:"id"`
	ResourceID    string `json:"resource_id"`
	ResourceType  string `json:"resource_type"` // space, base
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"` // user, department
	Role          string `json:"role"`           // owner, creator, editor, viewer, commenter
	CreatedBy     string `json:"created_by"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

// AddCollaboratorRequest 添加协作者请求DTO
type AddCollaboratorRequest struct {
	PrincipalID   string `json:"principal_id" binding:"required"`
	PrincipalType string `json:"principal_type" binding:"required,oneof=user department"`
	Role          string `json:"role" binding:"required,oneof=owner creator editor viewer commenter"`
}

// UpdateCollaboratorRequest 更新协作者请求DTO
type UpdateCollaboratorRequest struct {
	Role string `json:"role" binding:"required,oneof=owner creator editor viewer commenter"`
}

// ListCollaboratorsResponse 协作者列表响应
type ListCollaboratorsResponse struct {
	Collaborators []CollaboratorResponse `json:"collaborators"`
	Total         int                    `json:"total"`
}
