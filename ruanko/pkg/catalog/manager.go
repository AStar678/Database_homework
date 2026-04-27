package catalog

import (
	"fmt"
	"time"

	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
)

// Manager 统一的Catalog管理器
type Manager struct {
	DBMeta *DBMetaManager
}

// NewManager 创建Catalog管理器
func NewManager() *Manager {
	return &Manager{
		DBMeta: NewDBMetaManager(),
	}
}

// GetTableMeta 获取指定数据库的表元数据管理器
func (m *Manager) GetTableMeta(dbName string) (*TableMetaManager, error) {
	exists, _ := m.DBMeta.Exists(dbName)
	if !exists {
		return nil, common.NewError(1001, fmt.Sprintf("database '%s' not found", dbName))
	}
	return NewTableMetaManager(dbName), nil
}

// GetFieldMeta 获取字段元数据管理器
func (m *Manager) GetFieldMeta(dbName, tableName string) (*FieldDefManager, error) {
	tm, err := m.GetTableMeta(dbName)
	if err != nil {
		return nil, err
	}
	if !tm.Exists(tableName) {
		return nil, common.NewError(2001, fmt.Sprintf("table '%s' not found", tableName))
	}
	return NewFieldDefManager(dbName, tableName), nil
}

// CreateDatabase 创建数据库（包含系统库初始化）
func (m *Manager) CreateDatabase(name string) error {
	return m.DBMeta.Create(name)
}

// DropDatabase 删除数据库
func (m *Manager) DropDatabase(name string) error {
	return m.DBMeta.Drop(name)
}

// ListDatabases 列出所有数据库
func (m *Manager) ListDatabases() ([]*DatabaseBlock, error) {
	return m.DBMeta.LoadAll()
}

// GetDatabase 获取数据库
func (m *Manager) GetDatabase(name string) (*DatabaseBlock, error) {
	return m.DBMeta.Get(name)
}

// CreateTable 创建表（含字段）
func (m *Manager) CreateTable(dbName, tableName string, fields []*FieldBlock) error {
	tm, err := m.GetTableMeta(dbName)
	if err != nil {
		return err
	}
	if err := tm.Create(tableName); err != nil {
		return err
	}
	fm := NewFieldDefManager(dbName, tableName)
	for _, f := range fields {
		if err := fm.Add(f); err != nil {
			// 回滚：删除表
			tm.Drop(tableName)
			return err
		}
	}
	// 更新字段数
	tm.Update(tableName, func(tb *TableBlock) {
		tb.FieldNum = int32(len(fields))
		tb.Mtime = int32(time.Now().Unix())
	})
	return nil
}

// DropTable 删除表
func (m *Manager) DropTable(dbName, tableName string) error {
	tm, err := m.GetTableMeta(dbName)
	if err != nil {
		return err
	}
	return tm.Drop(tableName)
}

// GetTable 获取表信息
func (m *Manager) GetTable(dbName, tableName string) (*TableBlock, error) {
	tm, err := m.GetTableMeta(dbName)
	if err != nil {
		return nil, err
	}
	return tm.Get(tableName)
}

// ListTables 列出所有表
func (m *Manager) ListTables(dbName string) ([]*TableBlock, error) {
	tm, err := m.GetTableMeta(dbName)
	if err != nil {
		return nil, err
	}
	return tm.LoadAll()
}

// AddField 添加字段
func (m *Manager) AddField(dbName, tableName string, field *FieldBlock) error {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return err
	}
	if err := fm.Add(field); err != nil {
		return err
	}
	// 更新表字段数
	tm, _ := m.GetTableMeta(dbName)
	fields, _ := fm.LoadAll()
	tm.Update(tableName, func(tb *TableBlock) {
		tb.FieldNum = int32(len(fields))
		tb.Mtime = int32(time.Now().Unix())
	})
	return nil
}

// ModifyField 修改字段
func (m *Manager) ModifyField(dbName, tableName, oldName string, field *FieldBlock) error {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return err
	}
	return fm.Modify(oldName, field)
}

// DropField 删除字段
func (m *Manager) DropField(dbName, tableName, fieldName string) error {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return err
	}
	if err := fm.Drop(fieldName); err != nil {
		return err
	}
	// 更新表字段数
	tm, _ := m.GetTableMeta(dbName)
	fields, _ := fm.LoadAll()
	tm.Update(tableName, func(tb *TableBlock) {
		tb.FieldNum = int32(len(fields))
		tb.Mtime = int32(time.Now().Unix())
	})
	return nil
}

// GetFields 获取表的所有字段
func (m *Manager) GetFields(dbName, tableName string) ([]*FieldBlock, error) {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return fm.LoadAll()
}

// GetField 获取单个字段
func (m *Manager) GetField(dbName, tableName, fieldName string) (*FieldBlock, error) {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return fm.Get(fieldName)
}

// OpenRecordFile 打开表的记录文件
func (m *Manager) OpenRecordFile(dbName, tableName string) (*storage.RecordFile, error) {
	path := common.TableRecordFile(dbName, tableName)
	return storage.OpenRecordFile(path)
}

// GetFieldDefs 获取存储层字段定义
func (m *Manager) GetFieldDefs(dbName, tableName string) ([]storage.FieldDef, error) {
	fm, err := m.GetFieldMeta(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return fm.ToStorageDefs()
}
