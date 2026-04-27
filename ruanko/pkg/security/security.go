package security

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/ruanko/dbms/pkg/common"
)

// Manager 安全性管理器（简化版）
type Manager struct {
	mu    sync.RWMutex
	users map[string]string // username -> password hash
}

// NewManager 创建安全性管理器
func NewManager() *Manager {
	return &Manager{users: make(map[string]string)}
}

// hashPassword 密码哈希
func hashPassword(pwd string) string {
	h := sha256.New()
	h.Write([]byte(pwd))
	return hex.EncodeToString(h.Sum(nil))
}

// CreateUser 创建用户
func (m *Manager) CreateUser(username, password string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.users[username]; exists {
		return common.NewError(5001, fmt.Sprintf("user '%s' already exists", username))
	}
	m.users[username] = hashPassword(password)
	return nil
}

// Authenticate 验证用户
func (m *Manager) Authenticate(username, password string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hash, exists := m.users[username]
	return exists && hash == hashPassword(password)
}

// UserExists 检查用户是否存在
func (m *Manager) UserExists(username string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.users[username]
	return exists
}
