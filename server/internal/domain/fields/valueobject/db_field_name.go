package valueobject

import (
	"regexp"
	"strings"

	"github.com/easyspace-ai/luckdb/server/internal/domain/fields"

	"github.com/mozillazg/go-pinyin"
)

// DBFieldName 数据库字段名值对象
// 用于将字段名转换为数据库安全的列名
type DBFieldName struct {
	value string
}

// 数据库保留字（PostgreSQL）
var reservedWords = map[string]bool{
	"user": true, "table": true, "column": true, "index": true,
	"select": true, "insert": true, "update": true, "delete": true,
	"from": true, "where": true, "join": true, "order": true,
	"group": true, "having": true, "limit": true, "offset": true,
	"create": true, "drop": true, "alter": true, "database": true,
	"schema": true, "view": true, "trigger": true, "function": true,
}

// NewDBFieldName 创建数据库字段名
func NewDBFieldName(fieldName FieldName) (DBFieldName, error) {
	dbName := generateDBFieldName(fieldName.String())

	if err := validateDBFieldName(dbName); err != nil {
		return DBFieldName{}, err
	}

	return DBFieldName{value: dbName}, nil
}

// NewDBFieldNameFromString 从字符串创建数据库字段名
func NewDBFieldNameFromString(value string) (DBFieldName, error) {
	if err := validateDBFieldName(value); err != nil {
		return DBFieldName{}, err
	}

	return DBFieldName{value: value}, nil
}

// String 获取字符串值
func (db DBFieldName) String() string {
	return db.value
}

// Equals 比较两个数据库字段名是否相等
func (db DBFieldName) Equals(other DBFieldName) bool {
	return db.value == other.value
}

// generateDBFieldName 生成数据库字段名（参考原版 convertNameToValidCharacter 逻辑）
// 规则：
// 1. 中文转拼音（支持多语言）
// 2. 只保留字母、数字和下划线
// 3. 移除连续的下划线
// 4. 限制长度为40个字符
// 5. 如果为空或只有下划线，使用 "unnamed"
func generateDBFieldName(fieldName string) string {
	// 简化的拼音转换：使用ASCII范围的字符
	// 对于中文和其他非ASCII字符，使用简单的转换策略
	dbName := convertToASCII(fieldName)

	// 只保留字母、数字和下划线
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	dbName = reg.ReplaceAllString(dbName, "_")

	// 转换为小写（数据库字段名必须为小写）
	dbName = strings.ToLower(dbName)

	// 移除首尾下划线
	dbName = strings.Trim(dbName, "_")

	// 移除连续的下划线
	reg2 := regexp.MustCompile(`_{2,}`)
	dbName = reg2.ReplaceAllString(dbName, "_")

	// 如果为空或只有下划线，使用 "unnamed"
	if dbName == "" || regexp.MustCompile(`^_+$`).MatchString(dbName) {
		dbName = "unnamed"
	}

	// 确保以字母开头
	if len(dbName) > 0 && !isLetter(rune(dbName[0])) {
		dbName = "t" + dbName
	}

	// 限制长度为40个字符（参考原版逻辑）
	if len(dbName) > 40 {
		dbName = dbName[:40]
	}

	// 检查是否为保留字
	if reservedWords[strings.ToLower(dbName)] {
		dbName = "fld_" + dbName
	}

	return dbName
}

// convertToASCII 将中文等非ASCII字符转换为ASCII（使用专业的拼音库）
func convertToASCII(s string) string {
	// 配置拼音转换选项
	args := pinyin.NewArgs()
	args.Style = pinyin.Normal // 普通风格，不带音调
	args.Heteronym = false     // 不返回多音字
	args.Separator = ""        // 拼音之间的分隔符

	result := strings.Builder{}

	for _, r := range s {
		if r < 128 {
			// ASCII字符直接添加
			result.WriteRune(r)
		} else {
			// 使用专业的拼音库转换
			pinyinResult := pinyin.LazyConvert(string(r), &args)
			if len(pinyinResult) > 0 && pinyinResult[0] != "" {
				result.WriteString(pinyinResult[0])
			} else {
				// 如果无法转换，使用下划线
				result.WriteString("_")
			}
		}
	}

	return result.String()
}

// validateDBFieldName 验证数据库字段名
func validateDBFieldName(name string) error {
	if name == "" {
		return fields.ErrInvalidDBFieldName
	}

	// 检查长度（PostgreSQL 限制 63 字符）
	if len(name) > 63 {
		return fields.ErrInvalidDBFieldName
	}

	// 检查是否只包含字母、数字和下划线
	matched, _ := regexp.MatchString(`^[a-z][a-z0-9_]*$`, name)
	if !matched {
		return fields.ErrInvalidDBFieldName
	}

	return nil
}

// isLetter 检查是否为字母
func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}
