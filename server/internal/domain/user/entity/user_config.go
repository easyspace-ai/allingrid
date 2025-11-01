package entity

import (
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"

	"github.com/google/uuid"
)

// UserConfig 用户配置实体
// 存储用户个人偏好设置
type UserConfig struct {
	// 基础信息
	id     string
	userID string

	// 偏好设置
	timezone   string // 用户时区，如 "Asia/Shanghai", "America/New_York"
	language   string // 首选语言，如 "zh-CN", "en-US"
	dateFormat string // 日期格式偏好，如 "YYYY-MM-DD", "MM/DD/YYYY"
	timeFormat string // 时间格式偏好：12h, 24h

	// 审计字段
	createdAt time.Time
	updatedAt time.Time
}

// NewUserConfig 创建新用户配置（工厂方法）
func NewUserConfig(userID string) (*UserConfig, error) {
	// 验证
	if userID == "" {
		return nil, user.NewDomainError(
			"INVALID_USER_ID",
			"user ID cannot be empty",
			nil,
		)
	}

	now := time.Now()

	return &UserConfig{
		id:         uuid.New().String(),
		userID:     userID,
		timezone:   "UTC",        // 默认UTC时区
		language:   "en-US",      // 默认英语
		dateFormat: "YYYY-MM-DD", // 默认ISO日期格式
		timeFormat: "24h",        // 默认24小时制
		createdAt:  now,
		updatedAt:  now,
	}, nil
}

// ID 获取配置ID
func (uc *UserConfig) ID() string {
	return uc.id
}

// UserID 获取用户ID
func (uc *UserConfig) UserID() string {
	return uc.userID
}

// Timezone 获取时区
func (uc *UserConfig) Timezone() string {
	return uc.timezone
}

// Language 获取语言
func (uc *UserConfig) Language() string {
	return uc.language
}

// DateFormat 获取日期格式
func (uc *UserConfig) DateFormat() string {
	return uc.dateFormat
}

// TimeFormat 获取时间格式
func (uc *UserConfig) TimeFormat() string {
	return uc.timeFormat
}

// CreatedAt 获取创建时间
func (uc *UserConfig) CreatedAt() time.Time {
	return uc.createdAt
}

// UpdatedAt 获取更新时间
func (uc *UserConfig) UpdatedAt() time.Time {
	return uc.updatedAt
}

// UpdateTimezone 更新时区
func (uc *UserConfig) UpdateTimezone(timezone string) error {
	// 简单验证
	if timezone == "" {
		return user.NewDomainError(
			"INVALID_TIMEZONE",
			"timezone cannot be empty",
			nil,
		)
	}

	uc.timezone = timezone
	uc.updatedAt = time.Now()
	return nil
}

// UpdateLanguage 更新语言
func (uc *UserConfig) UpdateLanguage(language string) error {
	if language == "" {
		return user.NewDomainError(
			"INVALID_LANGUAGE",
			"language cannot be empty",
			nil,
		)
	}

	uc.language = language
	uc.updatedAt = time.Now()
	return nil
}

// UpdateDateFormat 更新日期格式
func (uc *UserConfig) UpdateDateFormat(dateFormat string) error {
	// 验证日期格式
	validFormats := []string{"YYYY-MM-DD", "MM/DD/YYYY", "DD/MM/YYYY", "YYYY年MM月DD日"}
	isValid := false
	for _, vf := range validFormats {
		if dateFormat == vf {
			isValid = true
			break
		}
	}

	if !isValid {
		return user.NewDomainError(
			"INVALID_DATE_FORMAT",
			"unsupported date format",
			nil,
		)
	}

	uc.dateFormat = dateFormat
	uc.updatedAt = time.Now()
	return nil
}

// UpdateTimeFormat 更新时间格式
func (uc *UserConfig) UpdateTimeFormat(timeFormat string) error {
	if timeFormat != "12h" && timeFormat != "24h" {
		return user.NewDomainError(
			"INVALID_TIME_FORMAT",
			"time format must be '12h' or '24h'",
			nil,
		)
	}

	uc.timeFormat = timeFormat
	uc.updatedAt = time.Now()
	return nil
}

// Update 批量更新配置
func (uc *UserConfig) Update(timezone, language, dateFormat, timeFormat string) error {
	if timezone != "" {
		if err := uc.UpdateTimezone(timezone); err != nil {
			return err
		}
	}

	if language != "" {
		if err := uc.UpdateLanguage(language); err != nil {
			return err
		}
	}

	if dateFormat != "" {
		if err := uc.UpdateDateFormat(dateFormat); err != nil {
			return err
		}
	}

	if timeFormat != "" {
		if err := uc.UpdateTimeFormat(timeFormat); err != nil {
			return err
		}
	}

	uc.updatedAt = time.Now()
	return nil
}
