package utils

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// ID生成相关常量
const (
	// Alphabet nanoid字母表
	Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	// DefaultIDLength 默认ID长度
	DefaultIDLength = 21

	// ID前缀
	UserIDPrefix       = "usr"
	AccountIDPrefix    = "acc"
	SpaceIDPrefix      = "spc"
	BaseIDPrefix       = "bse"
	TableIDPrefix      = "tbl"
	FieldIDPrefix      = "fld"
	RecordIDPrefix     = "rec"
	ViewIDPrefix       = "viw"
	DashboardIDPrefix  = "dsb"
	PluginIDPrefix     = "plg"
	AttachmentIDPrefix = "att"
	TokenIDPrefix      = "tkn"
	SessionIDPrefix    = "ses"
)

// IDGenerator ID生成器接口
type IDGenerator interface {
	Generate() string
	GenerateWithPrefix(prefix string) string
	GenerateNanoID(length int) string
}

// NanoIDGenerator NanoID生成器
type NanoIDGenerator struct {
	alphabet string
	length   int
}

// NewIDGenerator 创建ID生成器
func NewIDGenerator() IDGenerator {
	return &NanoIDGenerator{
		alphabet: Alphabet,
		length:   DefaultIDLength,
	}
}

// Generate 生成ID
func (g *NanoIDGenerator) Generate() string {
	return g.GenerateNanoID(g.length)
}

// GenerateWithPrefix 生成带前缀的ID
func (g *NanoIDGenerator) GenerateWithPrefix(prefix string) string {
	id := g.GenerateNanoID(g.length)
	return fmt.Sprintf("%s_%s", prefix, id)
}

// GenerateNanoID 生成指定长度的NanoID
func (g *NanoIDGenerator) GenerateNanoID(length int) string {
	if length <= 0 {
		length = DefaultIDLength
	}

	alphabetLen := int64(len(g.alphabet))
	mask := (2 << uint(findMSB(alphabetLen-1))) - 1
	step := int(float64(mask*length) / float64(alphabetLen) * 1.6)

	id := make([]byte, length)

	for i := 0; i < length; {
		randomBytes := make([]byte, step)
		if _, err := rand.Read(randomBytes); err != nil {
			// 如果随机数生成失败，使用时间戳+随机字符
			return g.fallbackID(length)
		}

		for j := 0; j < step && i < length; j++ {
			byteValue := int(randomBytes[j]) & mask
			if byteValue < len(g.alphabet) {
				id[i] = g.alphabet[byteValue]
				i++
			}
		}
	}

	return string(id)
}

// findMSB 找到最高有效位
func findMSB(n int64) int {
	msb := 0
	for n > 0 {
		n >>= 1
		msb++
	}
	return msb - 1
}

// fallbackID 生成备用ID（当随机数生成失败时）
func (g *NanoIDGenerator) fallbackID(length int) string {
	// 使用时间戳 + 随机字符作为备用方案
	timestamp := time.Now().UnixNano()
	timeStr := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", timestamp)))
	timeStr = strings.ReplaceAll(timeStr, "=", "")

	if len(timeStr) >= length {
		return timeStr[:length]
	}

	// 补充随机字符
	remaining := length - len(timeStr)
	randomPart := make([]byte, remaining)
	for i := 0; i < remaining; i++ {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(g.alphabet))))
		randomPart[i] = g.alphabet[n.Int64()]
	}

	return timeStr + string(randomPart)
}

// 全局ID生成器实例
var defaultGenerator = NewIDGenerator()

// 便捷函数

// GenerateID 生成ID
func GenerateID() string {
	return defaultGenerator.Generate()
}

// GenerateIDWithPrefix 生成带前缀的ID
func GenerateIDWithPrefix(prefix string) string {
	return defaultGenerator.GenerateWithPrefix(prefix)
}

// GenerateNanoID 生成指定长度的NanoID
func GenerateNanoID(length int) string {
	return defaultGenerator.GenerateNanoID(length)
}

// 特定类型ID生成函数

// GenerateUserID 生成用户ID
func GenerateUserID() string {
	return GenerateIDWithPrefix(UserIDPrefix)
}

// GenerateAccountID 生成账户ID
func GenerateAccountID() string {
	return GenerateIDWithPrefix(AccountIDPrefix)
}

// GenerateSpaceID 生成空间ID
func GenerateSpaceID() string {
	return GenerateIDWithPrefix(SpaceIDPrefix)
}

// GenerateBaseID 生成基础ID
func GenerateBaseID() string {
	return GenerateIDWithPrefix(BaseIDPrefix)
}

// GenerateTableID 生成表格ID
func GenerateTableID() string {
	return GenerateIDWithPrefix(TableIDPrefix)
}

// GenerateFieldID 生成字段ID
func GenerateFieldID() string {
	return GenerateIDWithPrefix(FieldIDPrefix)
}

// GenerateRecordID 生成记录ID
func GenerateRecordID() string {
	return GenerateIDWithPrefix(RecordIDPrefix)
}

// GenerateViewID 生成视图ID
func GenerateViewID() string {
	return GenerateIDWithPrefix(ViewIDPrefix)
}

// GenerateDashboardID 生成仪表板ID
func GenerateDashboardID() string {
	return GenerateIDWithPrefix(DashboardIDPrefix)
}

// GeneratePluginID 生成插件ID
func GeneratePluginID() string {
	return GenerateIDWithPrefix(PluginIDPrefix)
}

// GenerateAttachmentID 生成附件ID
func GenerateAttachmentID() string {
	return GenerateIDWithPrefix(AttachmentIDPrefix)
}

// GenerateTokenID 生成令牌ID
func GenerateTokenID() string {
	return GenerateIDWithPrefix(TokenIDPrefix)
}

// GenerateSessionID 生成会话ID
func GenerateSessionID() string {
	return GenerateIDWithPrefix(SessionIDPrefix)
}

// ValidateID 验证ID格式
func ValidateID(id string) bool {
	if len(id) == 0 {
		return false
	}

	// 检查是否包含前缀
	if strings.Contains(id, "_") {
		parts := strings.SplitN(id, "_", 2)
		if len(parts) != 2 {
			return false
		}

		prefix := parts[0]
		idPart := parts[1]

		// 验证前缀
		validPrefixes := []string{
			UserIDPrefix, AccountIDPrefix, SpaceIDPrefix, BaseIDPrefix,
			TableIDPrefix, FieldIDPrefix, RecordIDPrefix, ViewIDPrefix,
			DashboardIDPrefix, PluginIDPrefix, AttachmentIDPrefix,
			TokenIDPrefix, SessionIDPrefix,
		}

		validPrefix := false
		for _, validPref := range validPrefixes {
			if prefix == validPref {
				validPrefix = true
				break
			}
		}

		if !validPrefix {
			return false
		}

		// 验证ID部分
		return validateIDPart(idPart)
	}

	// 没有前缀，直接验证整个ID
	return validateIDPart(id)
}

// validateIDPart 验证ID部分是否有效
func validateIDPart(idPart string) bool {
	if len(idPart) < 10 || len(idPart) > 30 {
		return false
	}

	// 检查字符是否都在字母表中
	for _, char := range idPart {
		found := false
		for _, alphabetChar := range Alphabet {
			if char == alphabetChar {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// ExtractIDPrefix 提取ID前缀
func ExtractIDPrefix(id string) string {
	if strings.Contains(id, "_") {
		parts := strings.SplitN(id, "_", 2)
		if len(parts) == 2 {
			return parts[0]
		}
	}
	return ""
}

// ExtractIDPart 提取ID部分(去除前缀)
func ExtractIDPart(id string) string {
	if strings.Contains(id, "_") {
		parts := strings.SplitN(id, "_", 2)
		if len(parts) == 2 {
			return parts[1]
		}
	}
	return id
}

// IsUserID 检查是否为用户ID
func IsUserID(id string) bool {
	return ExtractIDPrefix(id) == UserIDPrefix
}

// IsSpaceID 检查是否为空间ID
func IsSpaceID(id string) bool {
	return ExtractIDPrefix(id) == SpaceIDPrefix
}

// IsBaseID 检查是否为基础ID
func IsBaseID(id string) bool {
	return ExtractIDPrefix(id) == BaseIDPrefix
}

// IsTableID 检查是否为表格ID
func IsTableID(id string) bool {
	return ExtractIDPrefix(id) == TableIDPrefix
}

// GenerateDataHash 生成数据哈希值
func GenerateDataHash(data map[string]interface{}, version int64) string {
	// 简单的哈希实现，实际项目中应该使用更强的哈希算法
	hashData := fmt.Sprintf("%v_%d_%d", data, version, time.Now().UnixNano())
	return base64.URLEncoding.EncodeToString([]byte(hashData))[:16]
}
