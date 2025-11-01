package dto

// UserConfigResponse 用户配置响应DTO
type UserConfigResponse struct {
	ID         string `json:"id"`
	UserID     string `json:"user_id"`
	Timezone   string `json:"timezone"`
	Language   string `json:"language"`
	DateFormat string `json:"date_format"`
	TimeFormat string `json:"time_format"`
	UpdatedAt  string `json:"updated_at"`
}

// UpdateUserConfigRequest 更新用户配置请求DTO
type UpdateUserConfigRequest struct {
	Timezone   string `json:"timezone,omitempty" binding:"omitempty,oneof=UTC Asia/Shanghai America/New_York Europe/London"`
	Language   string `json:"language,omitempty" binding:"omitempty,oneof=en-US zh-CN ja-JP"`
	DateFormat string `json:"date_format,omitempty" binding:"omitempty,oneof='YYYY-MM-DD' 'MM/DD/YYYY' 'DD/MM/YYYY' 'YYYY年MM月DD日'"`
	TimeFormat string `json:"time_format,omitempty" binding:"omitempty,oneof=12h 24h"`
}
