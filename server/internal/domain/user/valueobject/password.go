package valueobject

import (
	"unicode"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user"

	"golang.org/x/crypto/bcrypt"
)

const (
	MinPasswordLength = 8
	MaxPasswordLength = 128
	BcryptCost        = 12
)

// Password 密码值对象（明文密码，用于创建和验证）
type Password struct {
	value string
}

// NewPassword 创建密码值对象
func NewPassword(value string) (Password, error) {
	if err := validatePassword(value); err != nil {
		return Password{}, err
	}

	return Password{value: value}, nil
}

// String 获取密码值（注意：这个方法应该谨慎使用）
func (p Password) String() string {
	return p.value
}

// Hash 生成密码哈希
func (p Password) Hash() (HashedPassword, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(p.value), BcryptCost)
	if err != nil {
		return HashedPassword{}, user.NewDomainError(
			"PASSWORD_HASH_FAILED",
			"failed to hash password",
			err,
		)
	}

	return HashedPassword{value: string(hash)}, nil
}

// validatePassword 验证密码强度
func validatePassword(password string) error {
	if password == "" {
		return user.ErrPasswordEmpty
	}

	if len(password) < MinPasswordLength {
		return user.ErrPasswordTooShort
	}

	if len(password) > MaxPasswordLength {
		return user.ErrPasswordTooLong
	}

	// 检查密码强度（至少包含3种类型的字符）
	var hasLower, hasUpper, hasDigit, hasSpecial bool

	for _, char := range password {
		switch {
		case unicode.IsLower(char):
			hasLower = true
		case unicode.IsUpper(char):
			hasUpper = true
		case unicode.IsDigit(char):
			hasDigit = true
		case unicode.IsPunct(char) || unicode.IsSymbol(char):
			hasSpecial = true
		}
	}

	charTypes := 0
	if hasLower {
		charTypes++
	}
	if hasUpper {
		charTypes++
	}
	if hasDigit {
		charTypes++
	}
	if hasSpecial {
		charTypes++
	}

	if charTypes < 3 {
		return user.ErrPasswordTooWeak
	}

	return nil
}

// HashedPassword 哈希后的密码值对象
type HashedPassword struct {
	value string
}

// NewHashedPassword 从已有的哈希创建（从数据库加载）
func NewHashedPassword(hash string) (HashedPassword, error) {
	if hash == "" {
		return HashedPassword{}, user.ErrInvalidPasswordHash
	}

	return HashedPassword{value: hash}, nil
}

// String 获取哈希值
func (hp HashedPassword) String() string {
	return hp.value
}

// Verify 验证密码
func (hp HashedPassword) Verify(password Password) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hp.value), []byte(password.String()))
	return err == nil
}

// Equals 比较两个哈希是否相等
func (hp HashedPassword) Equals(other HashedPassword) bool {
	return hp.value == other.value
}

// IsEmpty 检查是否为空
func (hp HashedPassword) IsEmpty() bool {
	return hp.value == ""
}
