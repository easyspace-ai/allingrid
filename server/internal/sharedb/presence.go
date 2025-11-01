package sharedb

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PresenceManagerImpl 在线状态管理器实现
type PresenceManagerImpl struct {
	presences sync.Map // channel -> map[clientID]PresenceData
	logger    *zap.Logger
	mu        sync.RWMutex
}

// NewPresenceManager 创建在线状态管理器
func NewPresenceManager(logger *zap.Logger) PresenceManager {
	return &PresenceManagerImpl{
		logger: logger,
	}
}

// Submit 提交在线状态
func (pm *PresenceManagerImpl) Submit(ctx context.Context, channel, clientID string, data PresenceData) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 获取或创建频道的在线状态映射
	presencesInterface, _ := pm.presences.LoadOrStore(channel, make(map[string]PresenceData))
	presences := presencesInterface.(map[string]PresenceData)

	// 更新在线状态
	presences[clientID] = data

	pm.logger.Debug("Presence submitted",
		zap.String("channel", channel),
		zap.String("client_id", clientID),
		zap.String("user_id", data.UserID))

	return nil
}

// GetPresences 获取在线状态
func (pm *PresenceManagerImpl) GetPresences(ctx context.Context, channel string) (map[string]PresenceData, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	presencesInterface, exists := pm.presences.Load(channel)
	if !exists {
		return make(map[string]PresenceData), nil
	}

	presences := presencesInterface.(map[string]PresenceData)

	// 过滤掉过期的在线状态（超过5分钟）
	now := time.Now().Unix()
	filteredPresences := make(map[string]PresenceData)
	for clientID, presence := range presences {
		if now-presence.Timestamp < 300 { // 5分钟
			filteredPresences[clientID] = presence
		}
	}

	return filteredPresences, nil
}

// RemovePresence 移除在线状态
func (pm *PresenceManagerImpl) RemovePresence(ctx context.Context, channel, clientID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	presencesInterface, exists := pm.presences.Load(channel)
	if !exists {
		return nil
	}

	presences := presencesInterface.(map[string]PresenceData)
	delete(presences, clientID)

	pm.logger.Debug("Presence removed",
		zap.String("channel", channel),
		zap.String("client_id", clientID))

	return nil
}

// Close 关闭管理器
func (pm *PresenceManagerImpl) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 清理所有在线状态
	pm.presences.Range(func(key, value interface{}) bool {
		pm.presences.Delete(key)
		return true
	})

	pm.logger.Info("Presence manager closed")
	return nil
}
