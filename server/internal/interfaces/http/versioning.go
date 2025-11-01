package http

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/easyspace-ai/luckdb/server/pkg/errors"
	"github.com/easyspace-ai/luckdb/server/pkg/response"

	"github.com/gin-gonic/gin"
)

// APIVersion API版本信息
type APIVersion struct {
	Major      int    `json:"major"`
	Minor      int    `json:"minor"`
	Patch      int    `json:"patch"`
	PreRelease string `json:"pre_release,omitempty"`
	Build      string `json:"build,omitempty"`
}

// String 返回版本字符串
func (v APIVersion) String() string {
	version := fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
	if v.PreRelease != "" {
		version += "-" + v.PreRelease
	}
	if v.Build != "" {
		version += "+" + v.Build
	}
	return version
}

// IsCompatibleWith 检查版本兼容性
func (v APIVersion) IsCompatibleWith(other APIVersion) bool {
	// 主版本号必须相同
	if v.Major != other.Major {
		return false
	}

	// 次版本号向后兼容
	if v.Minor < other.Minor {
		return false
	}

	return true
}

// VersionManager 版本管理器
type VersionManager struct {
	currentVersion     APIVersion
	supportedVersions  []APIVersion
	deprecatedVersions map[string]string // version -> deprecation message
}

// NewVersionManager 创建版本管理器
func NewVersionManager() *VersionManager {
	return &VersionManager{
		currentVersion: APIVersion{
			Major: 1,
			Minor: 0,
			Patch: 0,
		},
		supportedVersions: []APIVersion{
			{Major: 1, Minor: 0, Patch: 0},
		},
		deprecatedVersions: make(map[string]string),
	}
}

// GetCurrentVersion 获取当前版本
func (vm *VersionManager) GetCurrentVersion() APIVersion {
	return vm.currentVersion
}

// GetSupportedVersions 获取支持的版本列表
func (vm *VersionManager) GetSupportedVersions() []APIVersion {
	return vm.supportedVersions
}

// IsVersionSupported 检查版本是否支持
func (vm *VersionManager) IsVersionSupported(version APIVersion) bool {
	for _, supported := range vm.supportedVersions {
		if version.Major == supported.Major &&
			version.Minor == supported.Minor &&
			version.Patch == supported.Patch {
			return true
		}
	}
	return false
}

// IsVersionDeprecated 检查版本是否已弃用
func (vm *VersionManager) IsVersionDeprecated(version APIVersion) (bool, string) {
	versionStr := version.String()
	message, deprecated := vm.deprecatedVersions[versionStr]
	return deprecated, message
}

// AddSupportedVersion 添加支持的版本
func (vm *VersionManager) AddSupportedVersion(version APIVersion) {
	vm.supportedVersions = append(vm.supportedVersions, version)
}

// DeprecateVersion 弃用版本
func (vm *VersionManager) DeprecateVersion(version APIVersion, message string) {
	vm.deprecatedVersions[version.String()] = message
}

// ParseVersion 解析版本字符串
func ParseVersion(versionStr string) (APIVersion, error) {
	// 移除 'v' 前缀
	versionStr = strings.TrimPrefix(versionStr, "v")

	// 分离预发布和构建信息
	var preRelease, build string

	if idx := strings.Index(versionStr, "+"); idx != -1 {
		build = versionStr[idx+1:]
		versionStr = versionStr[:idx]
	}

	if idx := strings.Index(versionStr, "-"); idx != -1 {
		preRelease = versionStr[idx+1:]
		versionStr = versionStr[:idx]
	}

	// 解析主版本号.次版本号.修订号
	parts := strings.Split(versionStr, ".")
	if len(parts) != 3 {
		return APIVersion{}, fmt.Errorf("invalid version format: %s", versionStr)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return APIVersion{}, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return APIVersion{}, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return APIVersion{}, fmt.Errorf("invalid patch version: %s", parts[2])
	}

	return APIVersion{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: preRelease,
		Build:      build,
	}, nil
}

// VersionMiddleware 版本控制中间件
func VersionMiddleware(vm *VersionManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从多个来源获取版本信息
		var requestedVersion APIVersion
		var err error

		// 1. 从URL路径获取版本 (如 /api/v1/...)
		if pathVersion := extractVersionFromPath(c.Request.URL.Path); pathVersion != "" {
			requestedVersion, err = ParseVersion(pathVersion)
		} else if headerVersion := c.GetHeader("API-Version"); headerVersion != "" {
			// 2. 从HTTP头获取版本
			requestedVersion, err = ParseVersion(headerVersion)
		} else if queryVersion := c.Query("version"); queryVersion != "" {
			// 3. 从查询参数获取版本
			requestedVersion, err = ParseVersion(queryVersion)
		} else {
			// 4. 使用默认版本
			requestedVersion = vm.GetCurrentVersion()
		}

		if err != nil {
			response.Error(c, errors.ErrBadRequest.WithDetails("Invalid API version format: "+err.Error()))
			c.Abort()
			return
		}

		// 检查版本是否支持
		if !vm.IsVersionSupported(requestedVersion) {
			response.Error(c, errors.ErrBadRequest.WithDetails(map[string]interface{}{
				"error":              "Unsupported API version",
				"requested_version":  requestedVersion.String(),
				"supported_versions": vm.GetSupportedVersions(),
			}))
			c.Abort()
			return
		}

		// 检查版本是否已弃用
		if deprecated, message := vm.IsVersionDeprecated(requestedVersion); deprecated {
			c.Header("API-Deprecated", "true")
			c.Header("API-Deprecation-Message", message)
			c.Header("API-Current-Version", vm.GetCurrentVersion().String())
		}

		// 设置版本信息到上下文
		c.Set("api_version", requestedVersion)
		c.Set("api_version_string", requestedVersion.String())

		// 设置响应头
		c.Header("API-Version", requestedVersion.String())
		c.Header("API-Current-Version", vm.GetCurrentVersion().String())

		c.Next()
	}
}

// extractVersionFromPath 从路径中提取版本信息
func extractVersionFromPath(path string) string {
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if strings.HasPrefix(part, "v") && len(part) > 1 {
			// 简单的版本格式检查
			if strings.Contains(part, ".") || len(part) == 2 {
				return part
			}
		}
	}
	return ""
}

// GetAPIVersionFromContext 从上下文获取API版本
func GetAPIVersionFromContext(c *gin.Context) APIVersion {
	if version, exists := c.Get("api_version"); exists {
		if apiVersion, ok := version.(APIVersion); ok {
			return apiVersion
		}
	}
	// 返回默认版本
	return APIVersion{Major: 1, Minor: 0, Patch: 0}
}

// VersionInfo 版本信息响应
type VersionInfo struct {
	Current    APIVersion   `json:"current"`
	Supported  []APIVersion `json:"supported"`
	Deprecated []string     `json:"deprecated,omitempty"`
}

// GetVersionInfo 获取版本信息处理器
func GetVersionInfo(vm *VersionManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		var deprecated []string
		for version := range vm.deprecatedVersions {
			deprecated = append(deprecated, version)
		}

		info := VersionInfo{
			Current:    vm.GetCurrentVersion(),
			Supported:  vm.GetSupportedVersions(),
			Deprecated: deprecated,
		}

		response.SuccessWithMessage(c, info, "API version information")
	}
}

// 全局版本管理器
var globalVersionManager *VersionManager

// GetGlobalVersionManager 获取全局版本管理器
func GetGlobalVersionManager() *VersionManager {
	if globalVersionManager == nil {
		globalVersionManager = NewVersionManager()
	}
	return globalVersionManager
}

// SetGlobalVersionManager 设置全局版本管理器
func SetGlobalVersionManager(vm *VersionManager) {
	globalVersionManager = vm
}
