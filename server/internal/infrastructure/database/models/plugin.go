package models

import (
	"time"
)

// Plugin 插件模型
type Plugin struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string    `gorm:"type:varchar(255);not null" json:"name"`
	Description      *string   `gorm:"type:text" json:"description"`
	DetailDesc       *string   `gorm:"column:detail_desc;type:text" json:"detail_desc"`
	Logo             string    `gorm:"type:varchar(500);not null" json:"logo"`
	HelpURL          *string   `gorm:"column:help_url;type:varchar(500)" json:"help_url"`
	Status           string    `gorm:"type:varchar(20);not null" json:"status"`
	Positions        string    `gorm:"type:text;not null" json:"positions"`
	URL              *string   `gorm:"type:varchar(500)" json:"url"`
	Secret           string    `gorm:"type:varchar(255);unique;not null" json:"secret"`
	MaskedSecret     string    `gorm:"column:masked_secret;type:varchar(255);not null" json:"masked_secret"`
	I18n             *string   `gorm:"type:text" json:"i18n"`
	Config           *string   `gorm:"type:text" json:"config"`
	PluginUser       *string   `gorm:"column:plugin_user;type:varchar(30)" json:"plugin_user"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`

	// 关联关系
	PluginInstalls []PluginInstall `gorm:"foreignKey:PluginID" json:"plugin_installs,omitempty"`
}

// TableName 指定表名
func (Plugin) TableName() string {
	return "plugin"
}

// PluginInstall 插件安装模型
type PluginInstall struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	PluginID         string    `gorm:"column:plugin_id;type:varchar(30);not null" json:"plugin_id"`
	BaseID           string    `gorm:"column:base_id;type:varchar(30);not null" json:"base_id"`
	Name             string    `gorm:"type:varchar(255);not null" json:"name"`
	PositionID       string    `gorm:"column:position_id;type:varchar(30);not null" json:"position_id"`
	Position         string    `gorm:"type:varchar(50);not null" json:"position"`
	Storage          *string   `gorm:"type:text" json:"storage"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`

	// 关联关系
	Plugin Plugin `gorm:"foreignKey:PluginID;references:ID" json:"plugin,omitempty"`
}

// TableName 指定表名
func (PluginInstall) TableName() string {
	return "plugin_install"
}

// PluginPanel 插件面板模型
type PluginPanel struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	Name             string    `gorm:"type:varchar(255);not null" json:"name"`
	TableID          string    `gorm:"column:table_id;type:varchar(30);not null" json:"table_id"`
	Layout           *string   `gorm:"type:text" json:"layout"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`

	// 关联关系
	Table Table `gorm:"foreignKey:TableID;references:ID" json:"table,omitempty"`
}

// TableName 指定表名
func (PluginPanel) TableName() string {
	return "plugin_panel"
}

// PluginContextMenu 插件上下文菜单模型
type PluginContextMenu struct {
	ID               string    `gorm:"primaryKey;type:varchar(30)" json:"id"`
	TableID          string    `gorm:"column:table_id;type:varchar(30);not null" json:"table_id"`
	PluginInstallID  string    `gorm:"column:plugin_install_id;type:varchar(30);unique;not null" json:"plugin_install_id"`
	Order            float64   `gorm:"column:order;not null" json:"order"`
	CreatedTime      time.Time `gorm:"autoCreateTime;column:created_time" json:"created_time"`
	CreatedBy        string    `gorm:"column:created_by;type:varchar(30);not null" json:"created_by"`
	LastModifiedTime time.Time `gorm:"autoUpdateTime;column:last_modified_time" json:"last_modified_time"`
	LastModifiedBy   *string   `gorm:"column:last_modified_by;type:varchar(50)" json:"last_modified_by"`

	// 关联关系
	Table Table `gorm:"foreignKey:TableID;references:ID" json:"table,omitempty"`
}

// TableName 指定表名
func (PluginContextMenu) TableName() string {
	return "plugin_context_menu"
}
