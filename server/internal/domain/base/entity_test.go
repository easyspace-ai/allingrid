package base

import (
	"testing"

	"github.com/easyspace-ai/luckdb/server/internal/domain/base/entity"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewBase(t *testing.T) {
	name := "测试基础"
	icon := "📊"
	spaceID := uuid.New().String()
	createdBy := uuid.New().String()

	base, err := entity.NewBase(name, icon, spaceID, createdBy)

	assert.NoError(t, err)
	assert.NotNil(t, base)
	assert.NotEmpty(t, base.ID)
	assert.Equal(t, name, base.Name)
	assert.Equal(t, icon, base.Icon)
	assert.Equal(t, spaceID, base.SpaceID)
	assert.Equal(t, createdBy, base.CreatedBy)
	assert.NotZero(t, base.CreatedAt)
	assert.NotZero(t, base.UpdatedAt)
}

func TestNewBase_Validation(t *testing.T) {
	t.Run("Empty name should fail", func(t *testing.T) {
		_, err := entity.NewBase("", "📊", uuid.New().String(), uuid.New().String())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "base name cannot be empty")
	})

	t.Run("Empty space ID should fail", func(t *testing.T) {
		_, err := entity.NewBase("测试基础", "📊", "", uuid.New().String())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "space ID cannot be empty")
	})

	t.Run("Empty created by should fail", func(t *testing.T) {
		_, err := entity.NewBase("测试基础", "📊", uuid.New().String(), "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "creator ID cannot be empty")
	})
}

func TestBase_UpdateName(t *testing.T) {
	base := createTestBase()
	newName := "新名称"

	err := base.UpdateName(newName)
	assert.NoError(t, err)
	assert.Equal(t, newName, base.Name)
}

func TestBase_UpdateIcon(t *testing.T) {
	base := createTestBase()
	newIcon := "🎯"

	base.UpdateIcon(newIcon)
	assert.Equal(t, newIcon, base.Icon)
}

// 辅助函数
func createTestBase() *entity.Base {
	name := "测试基础"
	icon := "📊"
	spaceID := uuid.New().String()
	createdBy := uuid.New().String()

	base, _ := entity.NewBase(name, icon, spaceID, createdBy)
	return base
}
