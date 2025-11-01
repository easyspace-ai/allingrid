package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/easyspace-ai/luckdb/server/internal/mcp/protocol"
)

// SessionService 会话服务
type SessionService struct {
	repo   SessionRepository
	config *SessionConfig
}

// SessionRepository 会话仓储接口
type SessionRepository interface {
	Create(ctx context.Context, session *Session) error
	GetByID(ctx context.Context, id string) (*Session, error)
	Update(ctx context.Context, session *Session) error
	Delete(ctx context.Context, id string) error
	DeleteByUserID(ctx context.Context, userID string) error
	CleanupExpired(ctx context.Context) error
}

// SessionConfig 会话配置
type SessionConfig struct {
	Enabled    bool          `json:"enabled"`
	CookieName string        `json:"cookie_name"`
	Secure     bool          `json:"secure"`
	HTTPOnly   bool          `json:"http_only"`
	SameSite   string        `json:"same_site"`
	MaxAge     time.Duration `json:"max_age"`
}

// Session 会话结构
type Session struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	IPAddress string    `json:"ip_address"`
	UserAgent string    `json:"user_agent"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	IsActive  bool      `json:"is_active"`
}

// NewSessionService 创建新的会话服务
func NewSessionService(repo SessionRepository, config *SessionConfig) *SessionService {
	return &SessionService{
		repo:   repo,
		config: config,
	}
}

// CreateSession 创建会话
func (s *SessionService) CreateSession(ctx context.Context, userID, ipAddress, userAgent string) (*Session, error) {
	// 生成会话 ID
	sessionID, err := s.generateSessionID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	session := &Session{
		ID:        sessionID,
		UserID:    userID,
		IPAddress: ipAddress,
		UserAgent: userAgent,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(s.config.MaxAge),
		IsActive:  true,
	}

	if err := s.repo.Create(ctx, session); err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return session, nil
}

// ValidateSession 验证会话
func (s *SessionService) ValidateSession(ctx context.Context, sessionID string) (*Session, error) {
	session, err := s.repo.GetByID(ctx, sessionID)
	if err != nil {
		return nil, protocol.NewAuthenticationFailedError("Session not found")
	}

	// 检查会话是否过期
	if session.ExpiresAt.Before(time.Now()) {
		return nil, protocol.NewAuthenticationFailedError("Session expired")
	}

	// 检查会话是否激活
	if !session.IsActive {
		return nil, protocol.NewAuthenticationFailedError("Session is inactive")
	}

	return session, nil
}

// DestroySession 销毁会话
func (s *SessionService) DestroySession(ctx context.Context, sessionID string) error {
	return s.repo.Delete(ctx, sessionID)
}

// DestroyAllUserSessions 销毁用户的所有会话
func (s *SessionService) DestroyAllUserSessions(ctx context.Context, userID string) error {
	return s.repo.DeleteByUserID(ctx, userID)
}

// RefreshSession 刷新会话
func (s *SessionService) RefreshSession(ctx context.Context, sessionID string) (*Session, error) {
	session, err := s.repo.GetByID(ctx, sessionID)
	if err != nil {
		return nil, protocol.NewAuthenticationFailedError("Session not found")
	}

	// 检查会话是否激活
	if !session.IsActive {
		return nil, protocol.NewAuthenticationFailedError("Session is inactive")
	}

	// 延长过期时间
	session.ExpiresAt = time.Now().Add(s.config.MaxAge)

	if err := s.repo.Update(ctx, session); err != nil {
		return nil, fmt.Errorf("failed to refresh session: %w", err)
	}

	return session, nil
}

// generateSessionID 生成会话 ID
func (s *SessionService) generateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "sess_" + hex.EncodeToString(bytes), nil
}
