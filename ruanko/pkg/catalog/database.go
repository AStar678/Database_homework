package catalog

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
)

// DatabaseBlock 数据库基本信息结构
type DatabaseBlock struct {
	Name     string    // CHAR[128]
	Type     bool      // false=系统数据库, true=用户数据库
	Filename string    // CHAR[256] 数据库数据文件夹全路径
	Crtime   time.Time // DATETIME 创建时间
}

// DatabaseBlockSize 数据库块大小
func DatabaseBlockSize() int {
	return common.Align4(128 + 1 + 3 + 256 + 16) // 404 -> 404
}

// Serialize 序列化数据库块
func (db *DatabaseBlock) Serialize() []byte {
	var buf bytes.Buffer
	common.WriteFixedString(&buf, db.Name, 128)
	common.WriteBool(&buf, db.Type)
	buf.Write(make([]byte, 3)) // padding to align
	common.WriteFixedString(&buf, db.Filename, 256)
	common.WriteDateTime(&buf, db.Crtime)
	return buf.Bytes()
}

// DeserializeDatabaseBlock 反序列化数据库块
func DeserializeDatabaseBlock(data []byte) *DatabaseBlock {
	if len(data) < DatabaseBlockSize() {
		return nil
	}
	return &DatabaseBlock{
		Name:     common.ReadFixedString(data, 0, 128),
		Type:     common.ReadBool(data, 128),
		Filename: common.ReadFixedString(data, 132, 256),
		Crtime:   common.ReadDateTime(data, 388),
	}
}

// DBMetaManager 数据库元数据管理器
type DBMetaManager struct {
	path string
}

// NewDBMetaManager 创建数据库元数据管理器
func NewDBMetaManager() *DBMetaManager {
	return &DBMetaManager{path: common.DBMetaFile()}
}

// LoadAll 加载所有数据库信息
func (m *DBMetaManager) LoadAll() ([]*DatabaseBlock, error) {
	if !common.FileExists(m.path) {
		// 初始化系统数据库
		return m.initSystemDB()
	}
	bf, err := storage.OpenBlockFile(m.path)
	if err != nil {
		return nil, err
	}
	defer bf.Close()

	data, err := bf.ReadAllBlocks()
	if err != nil {
		return nil, err
	}
	blockSize := DatabaseBlockSize()
	var dbs []*DatabaseBlock
	for offset := 0; offset+blockSize <= len(data); offset += blockSize {
		db := DeserializeDatabaseBlock(data[offset : offset+blockSize])
		if db != nil && db.Name != "" {
			dbs = append(dbs, db)
		}
	}
	return dbs, nil
}

// initSystemDB 初始化系统数据库
func (m *DBMetaManager) initSystemDB() ([]*DatabaseBlock, error) {
	sysDB := &DatabaseBlock{
		Name:     common.SystemDBName,
		Type:     false, // 系统数据库
		Filename: common.DBDir(common.SystemDBName),
		Crtime:   time.Now(),
	}
	// 创建系统数据库目录和文件
	common.EnsureDir(common.DBDir(common.SystemDBName))
	// 创建 .tb 和 .log 文件
	os.Create(common.TableMetaFile(common.SystemDBName))
	os.Create(common.LogFile(common.SystemDBName))

	// 写入 ruanko.db
	bf, err := storage.OpenBlockFile(m.path)
	if err != nil {
		return nil, err
	}
	defer bf.Close()
	bf.Append(sysDB.Serialize())
	return []*DatabaseBlock{sysDB}, nil
}

// Get 获取指定数据库
func (m *DBMetaManager) Get(name string) (*DatabaseBlock, error) {
	dbs, err := m.LoadAll()
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.Name == name {
			return db, nil
		}
	}
	return nil, common.NewError(1001, fmt.Sprintf("database '%s' not found", name))
}

// Exists 检查数据库是否存在
func (m *DBMetaManager) Exists(name string) (bool, error) {
	_, err := m.Get(name)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// Create 创建数据库
func (m *DBMetaManager) Create(name string) error {
	if len(name) > common.MaxNameLen {
		return common.NewError(1002, "database name too long (max 128)")
	}
	exists, _ := m.Exists(name)
	if exists {
		return common.NewError(1003, fmt.Sprintf("database '%s' already exists", name))
	}

	db := &DatabaseBlock{
		Name:     name,
		Type:     true, // 用户数据库
		Filename: common.DBDir(name),
		Crtime:   time.Now(),
	}

	// 创建目录和文件
	common.EnsureDir(common.DBDir(name))
	os.Create(common.TableMetaFile(name))
	os.Create(common.LogFile(name))

	// 追加到 ruanko.db
	bf, err := storage.OpenBlockFile(m.path)
	if err != nil {
		return err
	}
	defer bf.Close()
	return bf.Append(db.Serialize())
}

// Drop 删除数据库
func (m *DBMetaManager) Drop(name string) error {
	if name == common.SystemDBName {
		return common.NewError(1004, "cannot drop system database 'Ruanko'")
	}
	dbs, err := m.LoadAll()
	if err != nil {
		return err
	}

	var found bool
	var newDbs []*DatabaseBlock
	for _, db := range dbs {
		if db.Name == name {
			found = true
			continue
		}
		newDbs = append(newDbs, db)
	}
	if !found {
		return common.NewError(1001, fmt.Sprintf("database '%s' not found", name))
	}

	// 重写 ruanko.db
	bf, err := storage.OpenBlockFile(m.path)
	if err != nil {
		return err
	}
	defer bf.Close()
	bf.Truncate(0)
	for _, db := range newDbs {
		bf.Append(db.Serialize())
	}

	// 删除数据库目录
	os.RemoveAll(common.DBDir(name))
	return nil
}
