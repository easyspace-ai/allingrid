package testing

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

// MockUserRepository 模拟用户仓储
type MockUserRepository struct {
	mock.Mock
}

func (m *MockUserRepository) Create(ctx context.Context, user interface{}) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserRepository) GetByID(ctx context.Context, id string) (interface{}, error) {
	args := m.Called(ctx, id)
	return args.Get(0), args.Error(1)
}

func (m *MockUserRepository) GetByEmail(ctx context.Context, email string) (interface{}, error) {
	args := m.Called(ctx, email)
	return args.Get(0), args.Error(1)
}

func (m *MockUserRepository) Update(ctx context.Context, user interface{}) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockUserRepository) List(ctx context.Context, filter interface{}) (interface{}, error) {
	args := m.Called(ctx, filter)
	return args.Get(0), args.Error(1)
}

func (m *MockUserRepository) Count(ctx context.Context, filter interface{}) (int64, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockUserRepository) Exists(ctx context.Context, filter interface{}) (bool, error) {
	args := m.Called(ctx, filter)
	return args.Bool(0), args.Error(1)
}

// MockSpaceRepository 模拟空间仓储
type MockSpaceRepository struct {
	mock.Mock
}

func (m *MockSpaceRepository) Create(ctx context.Context, space interface{}) error {
	args := m.Called(ctx, space)
	return args.Error(0)
}

func (m *MockSpaceRepository) GetByID(ctx context.Context, id string) (interface{}, error) {
	args := m.Called(ctx, id)
	return args.Get(0), args.Error(1)
}

func (m *MockSpaceRepository) Update(ctx context.Context, space interface{}) error {
	args := m.Called(ctx, space)
	return args.Error(0)
}

func (m *MockSpaceRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockSpaceRepository) List(ctx context.Context, filter interface{}) (interface{}, error) {
	args := m.Called(ctx, filter)
	return args.Get(0), args.Error(1)
}

func (m *MockSpaceRepository) Count(ctx context.Context, filter interface{}) (int64, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).(int64), args.Error(1)
}

// MockTableRepository 模拟表仓储
type MockTableRepository struct {
	mock.Mock
}

func (m *MockTableRepository) Create(ctx context.Context, table interface{}) error {
	args := m.Called(ctx, table)
	return args.Error(0)
}

func (m *MockTableRepository) GetByID(ctx context.Context, id string) (interface{}, error) {
	args := m.Called(ctx, id)
	return args.Get(0), args.Error(1)
}

func (m *MockTableRepository) Update(ctx context.Context, table interface{}) error {
	args := m.Called(ctx, table)
	return args.Error(0)
}

func (m *MockTableRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockTableRepository) List(ctx context.Context, filter interface{}) (interface{}, error) {
	args := m.Called(ctx, filter)
	return args.Get(0), args.Error(1)
}

func (m *MockTableRepository) Count(ctx context.Context, filter interface{}) (int64, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).(int64), args.Error(1)
}

// MockFieldRepository 模拟字段仓储
type MockFieldRepository struct {
	mock.Mock
}

func (m *MockFieldRepository) Create(ctx context.Context, field interface{}) error {
	args := m.Called(ctx, field)
	return args.Error(0)
}

func (m *MockFieldRepository) GetByID(ctx context.Context, id string) (interface{}, error) {
	args := m.Called(ctx, id)
	return args.Get(0), args.Error(1)
}

func (m *MockFieldRepository) Update(ctx context.Context, field interface{}) error {
	args := m.Called(ctx, field)
	return args.Error(0)
}

func (m *MockFieldRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockFieldRepository) List(ctx context.Context, filter interface{}) (interface{}, error) {
	args := m.Called(ctx, filter)
	return args.Get(0), args.Error(1)
}

func (m *MockFieldRepository) Count(ctx context.Context, filter interface{}) (int64, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).(int64), args.Error(1)
}

// MockUserDomainService 模拟用户领域服务
type MockUserDomainService struct {
	mock.Mock
}

func (m *MockUserDomainService) GetUser(ctx context.Context, userID string) (interface{}, error) {
	args := m.Called(ctx, userID)
	return args.Get(0), args.Error(1)
}

func (m *MockUserDomainService) CreateUser(ctx context.Context, user interface{}) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserDomainService) UpdateUser(ctx context.Context, user interface{}) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserDomainService) DeleteUser(ctx context.Context, userID string) error {
	args := m.Called(ctx, userID)
	return args.Error(0)
}

func (m *MockUserDomainService) ListUsers(ctx context.Context, filter interface{}) (interface{}, error) {
	args := m.Called(ctx, filter)
	return args.Get(0), args.Error(1)
}

func (m *MockUserDomainService) ValidateUser(ctx context.Context, user interface{}) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserDomainService) BulkCreateUsers(ctx context.Context, users interface{}) error {
	args := m.Called(ctx, users)
	return args.Error(0)
}

func (m *MockUserDomainService) BulkUpdateUsers(ctx context.Context, users interface{}) error {
	args := m.Called(ctx, users)
	return args.Error(0)
}

func (m *MockUserDomainService) BulkDeleteUsers(ctx context.Context, userIDs []string) error {
	args := m.Called(ctx, userIDs)
	return args.Error(0)
}

func (m *MockUserDomainService) ExportUsers(ctx context.Context, filter interface{}) ([]byte, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockUserDomainService) ImportUsers(ctx context.Context, data []byte) (interface{}, error) {
	args := m.Called(ctx, data)
	return args.Get(0), args.Error(1)
}

func (m *MockUserDomainService) GetUserStats(ctx context.Context) (interface{}, error) {
	args := m.Called(ctx)
	return args.Get(0), args.Error(1)
}

func (m *MockUserDomainService) GetUserActivity(ctx context.Context, userID string, limit int) (interface{}, error) {
	args := m.Called(ctx, userID, limit)
	return args.Get(0), args.Error(1)
}

func (m *MockUserDomainService) UpdateUserPreferences(ctx context.Context, userID string, prefs interface{}) error {
	args := m.Called(ctx, userID, prefs)
	return args.Error(0)
}

func (m *MockUserDomainService) GetUserPreferences(ctx context.Context, userID string) (interface{}, error) {
	args := m.Called(ctx, userID)
	return args.Get(0), args.Error(1)
}

// MockTokenService 模拟令牌服务
type MockTokenService struct {
	mock.Mock
}

func (m *MockTokenService) GenerateTokenPair(ctx context.Context, user interface{}, sessionID string) (interface{}, error) {
	args := m.Called(ctx, user, sessionID)
	return args.Get(0), args.Error(1)
}

func (m *MockTokenService) RefreshToken(ctx context.Context, refreshToken string, user interface{}) (interface{}, error) {
	args := m.Called(ctx, refreshToken, user)
	return args.Get(0), args.Error(1)
}

func (m *MockTokenService) ValidateToken(ctx context.Context, token string) (interface{}, error) {
	args := m.Called(ctx, token)
	return args.Get(0), args.Error(1)
}

func (m *MockTokenService) BlacklistToken(ctx context.Context, token string) error {
	args := m.Called(ctx, token)
	return args.Error(0)
}

func (m *MockTokenService) InvalidateUserTokens(ctx context.Context, userID string) error {
	args := m.Called(ctx, userID)
	return args.Error(0)
}

func (m *MockTokenService) GenerateAPIKey(ctx context.Context, userID, name string, expiresAt *time.Time) (string, error) {
	args := m.Called(ctx, userID, name, expiresAt)
	return args.String(0), args.Error(1)
}

func (m *MockTokenService) ValidateAPIKey(ctx context.Context, apiKey string) (string, error) {
	args := m.Called(ctx, apiKey)
	return args.String(0), args.Error(1)
}

func (m *MockTokenService) RevokeAPIKey(ctx context.Context, apiKey string) error {
	args := m.Called(ctx, apiKey)
	return args.Error(0)
}

// MockSessionService 模拟会话服务
type MockSessionService struct {
	mock.Mock
}

func (m *MockSessionService) CreateSession(ctx context.Context, userID string, session interface{}) (string, error) {
	args := m.Called(ctx, userID, session)
	return args.String(0), args.Error(1)
}

func (m *MockSessionService) GetSession(ctx context.Context, sessionID string) (interface{}, error) {
	args := m.Called(ctx, sessionID)
	return args.Get(0), args.Error(1)
}

func (m *MockSessionService) UpdateSessionActivity(ctx context.Context, sessionID string) error {
	args := m.Called(ctx, sessionID)
	return args.Error(0)
}

func (m *MockSessionService) InvalidateSession(ctx context.Context, sessionID string) error {
	args := m.Called(ctx, sessionID)
	return args.Error(0)
}

func (m *MockSessionService) GetUserActiveSessions(ctx context.Context, userID string) (interface{}, error) {
	args := m.Called(ctx, userID)
	return args.Get(0), args.Error(1)
}

func (m *MockSessionService) InvalidateAllUserSessions(ctx context.Context, userID string) error {
	args := m.Called(ctx, userID)
	return args.Error(0)
}

// MockCacheService 模拟缓存服务
type MockCacheService struct {
	mock.Mock
}

func (m *MockCacheService) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	args := m.Called(ctx, key, value, ttl)
	return args.Error(0)
}

func (m *MockCacheService) Get(ctx context.Context, key string) (interface{}, error) {
	args := m.Called(ctx, key)
	return args.Get(0), args.Error(1)
}

func (m *MockCacheService) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockCacheService) Clear(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockCacheService) Close() error {
	args := m.Called()
	return args.Error(0)
}
