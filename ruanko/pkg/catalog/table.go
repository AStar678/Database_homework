package catalog

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
)

// TableBlock 表格信息结构
type TableBlock struct {
	Name       string    // CHAR[128] 表格名称
	RecordNum  int32     // INTEGER 记录数
	FieldNum   int32     // INTEGER 字段数
	Tdf        string    // CHAR[256] 表格定义文件路径
	Tic        string    // CHAR[256] 完整性文件路径
	Trd        string    // CHAR[256] 记录文件路径
	Tid        string    // CHAR[256] 索引文件路径
	Crtime     time.Time // DATETIME 创建时间
	Mtime      int32     // INTEGER 最后修改时间 (文档如此，存unix timestamp)
}

// TableBlockSize 表块大小
func TableBlockSize() int {
	return common.Align4(128 + 4 + 4 + 256 + 256 + 256 + 256 + 16 + 4) // 1280 -> 1280
}

// Serialize 序列化表块
func (tb *TableBlock) Serialize() []byte {
	var buf bytes.Buffer
	common.WriteFixedString(&buf, tb.Name, 128)
	common.WriteInt(&buf, tb.RecordNum)
	common.WriteInt(&buf, tb.FieldNum)
	common.WriteFixedString(&buf, tb.Tdf, 256)
	common.WriteFixedString(&buf, tb.Tic, 256)
	common.WriteFixedString(&buf, tb.Trd, 256)
	common.WriteFixedString(&buf, tb.Tid, 256)
	common.WriteDateTime(&buf, tb.Crtime)
	common.WriteInt(&buf, tb.Mtime)
	return buf.Bytes()
}

// DeserializeTableBlock 反序列化表块
func DeserializeTableBlock(data []byte) *TableBlock {
	if len(data) < TableBlockSize() {
		return nil
	}
	return &TableBlock{
		Name:      common.ReadFixedString(data, 0, 128),
		RecordNum: common.ReadInt(data, 128),
		FieldNum:  common.ReadInt(data, 132),
		Tdf:       common.ReadFixedString(data, 136, 256),
		Tic:       common.ReadFixedString(data, 392, 256),
		Trd:       common.ReadFixedString(data, 648, 256),
		Tid:       common.ReadFixedString(data, 904, 256),
		Crtime:    common.ReadDateTime(data, 1160),
		Mtime:     common.ReadInt(data, 1176),
	}
}

// TableMetaManager 表元数据管理器
type TableMetaManager struct {
	dbName string
}

// NewTableMetaManager 创建表元数据管理器
func NewTableMetaManager(dbName string) *TableMetaManager {
	return &TableMetaManager{dbName: dbName}
}

func (m *TableMetaManager) path() string {
	return common.TableMetaFile(m.dbName)
}

// LoadAll 加载所有表信息
func (m *TableMetaManager) LoadAll() ([]*TableBlock, error) {
	if !common.FileExists(m.path()) {
		return []*TableBlock{}, nil
	}
	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return nil, err
	}
	defer bf.Close()

	data, err := bf.ReadAllBlocks()
	if err != nil {
		return nil, err
	}
	blockSize := TableBlockSize()
	var tables []*TableBlock
	for offset := 0; offset+blockSize <= len(data); offset += blockSize {
		tb := DeserializeTableBlock(data[offset : offset+blockSize])
		if tb != nil && tb.Name != "" {
			tables = append(tables, tb)
		}
	}
	return tables, nil
}

// Get 获取指定表
func (m *TableMetaManager) Get(name string) (*TableBlock, error) {
	tables, err := m.LoadAll()
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if t.Name == name {
			return t, nil
		}
	}
	return nil, common.NewError(2001, fmt.Sprintf("table '%s' not found in database '%s'", name, m.dbName))
}

// Exists 检查表是否存在
func (m *TableMetaManager) Exists(name string) bool {
	_, err := m.Get(name)
	return err == nil
}

// Create 创建表
func (m *TableMetaManager) Create(name string) error {
	if len(name) > common.MaxNameLen {
		return common.NewError(2002, "table name too long (max 128)")
	}
	if m.Exists(name) {
		return common.NewError(2003, fmt.Sprintf("table '%s' already exists", name))
	}

	now := time.Now()
	tb := &TableBlock{
		Name:      name,
		RecordNum: 0,
		FieldNum:  0,
		Tdf:       common.TableDefFile(m.dbName, name),
		Tic:       common.TableIntegrityFile(m.dbName, name),
		Trd:       common.TableRecordFile(m.dbName, name),
		Tid:       common.TableIndexFile(m.dbName, name),
		Crtime:    now,
		Mtime:     int32(now.Unix()),
	}

	// 创建表相关文件
	storage.OpenBlockFile(tb.Tdf)
	storage.OpenBlockFile(tb.Tic)
	storage.OpenBlockFile(tb.Trd)
	storage.OpenBlockFile(tb.Tid)

	// 追加到 [dbname].tb
	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	return bf.Append(tb.Serialize())
}

// Drop 删除表
func (m *TableMetaManager) Drop(name string) error {
	tables, err := m.LoadAll()
	if err != nil {
		return err
	}
	var target *TableBlock
	var newTables []*TableBlock
	for _, t := range tables {
		if t.Name == name {
			target = t
			continue
		}
		newTables = append(newTables, t)
	}
	if target == nil {
		return common.NewError(2001, fmt.Sprintf("table '%s' not found", name))
	}

	// 重写 [dbname].tb
	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	bf.Truncate(0)
	for _, t := range newTables {
		bf.Append(t.Serialize())
	}

	// 删除表相关文件
	storage.DeleteBlockFile(target.Tdf)
	storage.DeleteBlockFile(target.Tic)
	storage.DeleteBlockFile(target.Trd)
	storage.DeleteBlockFile(target.Tid)
	// 删除索引数据文件
	// TODO: 读取tid文件，删除所有ix文件
	return nil
}

// Update 更新表信息
func (m *TableMetaManager) Update(name string, updater func(*TableBlock)) error {
	tables, err := m.LoadAll()
	if err != nil {
		return err
	}
	found := false
	for _, t := range tables {
		if t.Name == name {
			updater(t)
			found = true
			break
		}
	}
	if !found {
		return common.NewError(2001, fmt.Sprintf("table '%s' not found", name))
	}

	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	bf.Truncate(0)
	for _, t := range tables {
		bf.Append(t.Serialize())
	}
	return nil
}
