package dto

import "time"

// UserActivity 用户活动信息
type UserActivity struct {
	UserID         string           `json:"userId"`         // ✅ 统一使用 camelCase
	TotalActions   int64            `json:"totalActions"`   // ✅ 统一使用 camelCase
	RecentActions  []ActionLog      `json:"recentActions"`  // ✅ 统一使用 camelCase
	ActiveDays     int              `json:"activeDays"`     // ✅ 统一使用 camelCase
	LastActiveDate time.Time        `json:"lastActiveDate"` // ✅ 统一使用 camelCase
	Statistics     map[string]int64 `json:"statistics"`
}

// ActionLog 操作日志
type ActionLog struct {
	Action    string    `json:"action"`
	Target    string    `json:"target"`
	Timestamp time.Time `json:"timestamp"`
}

// UserPreferences 用户偏好设置
type UserPreferences struct {
	Language       string                 `json:"language"`
	Timezone       string                 `json:"timezone"`
	Theme          string                 `json:"theme"`
	Notifications  map[string]bool        `json:"notifications"`
	CustomSettings map[string]interface{} `json:"customSettings"` // ✅ 统一使用 camelCase
}
