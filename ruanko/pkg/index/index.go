package index

// Manager 索引管理器（简化框架实现）
type Manager struct {
	// 内存索引映射: 表名 -> 索引名 -> 字段值 -> 记录偏移列表
	indexes map[string]map[string]map[string][]int64
}

// NewManager 创建索引管理器
func NewManager() *Manager {
	return &Manager{indexes: make(map[string]map[string]map[string][]int64)}
}

// AddIndex 添加索引（内存中）
func (m *Manager) AddIndex(table, indexName, field string) {
	if m.indexes[table] == nil {
		m.indexes[table] = make(map[string]map[string][]int64)
	}
	m.indexes[table][indexName] = make(map[string][]int64)
}

// UpdateIndex 更新索引值
func (m *Manager) UpdateIndex(table, indexName, fieldValue string, offset int64) {
	if m.indexes[table] == nil || m.indexes[table][indexName] == nil {
		return
	}
	m.indexes[table][indexName][fieldValue] = append(m.indexes[table][indexName][fieldValue], offset)
}

// RemoveIndex 移除索引值
func (m *Manager) RemoveIndex(table, indexName, fieldValue string, offset int64) {
	if m.indexes[table] == nil || m.indexes[table][indexName] == nil {
		return
	}
	list := m.indexes[table][indexName][fieldValue]
	for i, v := range list {
		if v == offset {
			m.indexes[table][indexName][fieldValue] = append(list[:i], list[i+1:]...)
			break
		}
	}
}

// Lookup 索引查找
func (m *Manager) Lookup(table, indexName, fieldValue string) []int64 {
	if m.indexes[table] == nil || m.indexes[table][indexName] == nil {
		return nil
	}
	return m.indexes[table][indexName][fieldValue]
}
