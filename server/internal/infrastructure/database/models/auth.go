package models

import "time"

type AccessToken struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string     `gorm:"type:varchar(255);not null" json:"name"`
	Description      *string    `gorm:"type:text" json:"description"`
	UserID           string     `gorm:"column:user_id;type:varchar(30);index;not null" json:"user_id"`
	Scopes           string     `gorm:"type:text;not null" json:"scopes"`
	SpaceIDs         *string    `gorm:"column:space_ids;type:text" json:"space_ids"`
	BaseIDs          *string    `gorm:"column:base_ids;type:text" json:"base_ids"`
	Sign             string     `gorm:"type:varchar(255);not null" json:"sign"`
	ClientID         *string    `gorm:"column:client_id;type:varchar(50);index" json:"client_id"`
	HasFullAccess    *bool      `gorm:"column:has_full_access" json:"has_full_access"`
	ExpiredTime      time.Time  `gorm:"column:expired_time;not null" json:"expired_time"`
	LastUsedTime     *time.Time `gorm:"column:last_used_time" json:"last_used_time"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
}

func (AccessToken) TableName() string { return "access_token" }

type OAuthApp struct {
	ID               string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string     `gorm:"type:varchar(255);not null" json:"name"`
	Logo             *string    `gorm:"type:varchar(500)" json:"logo"`
	Homepage         string     `gorm:"type:varchar(500);not null" json:"homepage"`
	Description      *string    `gorm:"type:text" json:"description"`
	ClientID         string     `gorm:"column:client_id;type:varchar(100);uniqueIndex;not null" json:"client_id"`
	RedirectUris     *string    `gorm:"column:redirect_uris;type:text" json:"redirect_uris"`
	Scopes           *string    `gorm:"type:text" json:"scopes"`
	CreatedTime      time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime *time.Time `gorm:"column:last_modified_time" json:"last_modified_time"`
	CreatedBy        string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
}

func (OAuthApp) TableName() string { return "oauth_app" }

type OAuthAppAuthorized struct {
	ID             string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	ClientID       string     `gorm:"column:client_id;type:varchar(100);not null" json:"client_id"`
	UserID         string     `gorm:"column:user_id;type:varchar(30);not null" json:"user_id"`
	AuthorizedTime *time.Time `gorm:"column:authorized_time" json:"authorized_time"`

	// unique(client_id, user_id)
	_ string `gorm:"uniqueIndex:uq_oauth_authorized_client_user"`
}

func (OAuthAppAuthorized) TableName() string { return "oauth_app_authorized" }

type OAuthAppSecret struct {
	ID           string     `gorm:"primaryKey;type:varchar(30)" json:"id"`
	ClientID     string     `gorm:"column:client_id;type:varchar(100);not null" json:"client_id"`
	Secret       string     `gorm:"type:varchar(255);uniqueIndex;not null" json:"secret"`
	MaskedSecret string     `gorm:"column:masked_secret;type:varchar(255);not null" json:"masked_secret"`
	CreatedTime  time.Time  `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy    string     `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastUsedTime *time.Time `gorm:"column:last_used_time" json:"last_used_time"`
}

func (OAuthAppSecret) TableName() string { return "oauth_app_secret" }

type OAuthAppToken struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	AppSecretID      string    `gorm:"column:app_secret_id;type:varchar(30);not null" json:"app_secret_id"`
	RefreshTokenSign string    `gorm:"column:refresh_token_sign;type:varchar(255);uniqueIndex;not null" json:"refresh_token_sign"`
	ExpiredTime      time.Time `gorm:"column:expired_time;not null" json:"expired_time"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
}

func (OAuthAppToken) TableName() string { return "oauth_app_token" }
