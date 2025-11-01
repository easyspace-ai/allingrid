package entity

import (
	"testing"

	"github.com/easyspace-ai/luckdb/server/internal/domain/user/valueobject"
	"github.com/stretchr/testify/assert"
)

func TestNewUser(t *testing.T) {
	userID := valueobject.NewUserID()
	email := valueobject.NewEmail("test@example.com")
	password := valueobject.NewPassword("SecurePass123!")
	name := "测试用户"

	user, err := NewUser(userID, email, password, name)

	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, userID, user.ID())
	assert.Equal(t, email, user.Email())
	assert.Equal(t, name, user.Name())
	assert.Equal(t, UserStatusActive, user.Status())
	assert.NotZero(t, user.CreatedAt())
	assert.NotZero(t, user.UpdatedAt())
}

func TestUser_UpdateName(t *testing.T) {
	user := createTestUser()
	newName := "新用户名"

	err := user.UpdateName(newName)
	assert.NoError(t, err)
	assert.Equal(t, newName, user.Name())
}

func TestUser_UpdateEmail(t *testing.T) {
	user := createTestUser()
	newEmail := valueobject.NewEmail("newemail@example.com")

	err := user.UpdateEmail(newEmail)
	assert.NoError(t, err)
	assert.Equal(t, newEmail, user.Email())
}

func TestUser_ChangePassword(t *testing.T) {
	user := createTestUser()
	newPassword := valueobject.NewPassword("NewSecurePass123!")

	err := user.ChangePassword(newPassword)
	assert.NoError(t, err)

	// 验证新密码
	assert.True(t, user.VerifyPassword(newPassword))
}

func TestUser_VerifyPassword(t *testing.T) {
	user := createTestUser()
	correctPassword := valueobject.NewPassword("SecurePass123!")
	wrongPassword := valueobject.NewPassword("WrongPass123!")

	assert.True(t, user.VerifyPassword(correctPassword))
	assert.False(t, user.VerifyPassword(wrongPassword))
}

func TestUser_Activate(t *testing.T) {
	user := createTestUser()
	user.Deactivate()

	err := user.Activate()
	assert.NoError(t, err)
	assert.Equal(t, UserStatusActive, user.Status())
}

func TestUser_Deactivate(t *testing.T) {
	user := createTestUser()

	err := user.Deactivate()
	assert.NoError(t, err)
	assert.Equal(t, UserStatusInactive, user.Status())
}

func TestUser_IsActive(t *testing.T) {
	user := createTestUser()
	assert.True(t, user.IsActive())

	user.Deactivate()
	assert.False(t, user.IsActive())
}

// 辅助函数
func createTestUser() *User {
	userID := valueobject.NewUserID()
	email := valueobject.NewEmail("test@example.com")
	password := valueobject.NewPassword("SecurePass123!")
	name := "测试用户"

	user, _ := NewUser(userID, email, password, name)
	return user
}
