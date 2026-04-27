package catalog

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
	"github.com/ruanko/dbms/pkg/types"
)

// Integrity constraints bitmask
const (
	ConstraintNotNull    = 1 << 0
	ConstraintPrimaryKey = 1 << 1
	ConstraintUnique     = 1 << 2
	ConstraintDefault    = 1 << 3
	ConstraintIdentity   = 1 << 4
	ConstraintForeignKey = 1 << 5
	ConstraintCheck      = 1 << 6
)

// FieldBlock 字段结构
type FieldBlock struct {
	Order       int32     // INTEGER 字段顺序
	Name        string    // CHAR[128] 字段名称
	Type        int32     // INTEGER 字段类型 (types.DataType)
	Param       int32     // INTEGER 字段类型参数
	Mtime       time.Time // DATETIME 最后修改时间
	Integrities int32     // INTEGER 完整性约束信息
}

// FieldBlockSize 字段块大小
func FieldBlockSize() int {
	return common.Align4(4 + 128 + 4 + 4 + 16 + 4) // 160 -> 160
}

// Serialize 序列化字段块
func (fb *FieldBlock) Serialize() []byte {
	var buf bytes.Buffer
	common.WriteInt(&buf, fb.Order)
	common.WriteFixedString(&buf, fb.Name, 128)
	common.WriteInt(&buf, fb.Type)
	common.WriteInt(&buf, fb.Param)
	common.WriteDateTime(&buf, fb.Mtime)
	common.WriteInt(&buf, fb.Integrities)
	return buf.Bytes()
}

// DeserializeFieldBlock 反序列化字段块
func DeserializeFieldBlock(data []byte) *FieldBlock {
	if len(data) < FieldBlockSize() {
		return nil
	}
	return &FieldBlock{
		Order:       common.ReadInt(data, 0),
		Name:        common.ReadFixedString(data, 4, 128),
		Type:        common.ReadInt(data, 132),
		Param:       common.ReadInt(data, 136),
		Mtime:       common.ReadDateTime(data, 140),
		Integrities: common.ReadInt(data, 156),
	}
}

// DataTypeVal 返回类型枚举值
func (fb *FieldBlock) DataTypeVal() types.DataType {
	return types.DataType(fb.Type)
}

// FieldDefManager 字段定义管理器
type FieldDefManager struct {
	dbName    string
	tableName string
}

// NewFieldDefManager 创建字段定义管理器
func NewFieldDefManager(dbName, tableName string) *FieldDefManager {
	return &FieldDefManager{dbName: dbName, tableName: tableName}
}

func (m *FieldDefManager) path() string {
	return common.TableDefFile(m.dbName, m.tableName)
}

// LoadAll 加载所有字段
func (m *FieldDefManager) LoadAll() ([]*FieldBlock, error) {
	if !common.FileExists(m.path()) {
		return []*FieldBlock{}, nil
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
	blockSize := FieldBlockSize()
	var fields []*FieldBlock
	for offset := 0; offset+blockSize <= len(data); offset += blockSize {
		fb := DeserializeFieldBlock(data[offset : offset+blockSize])
		if fb != nil && fb.Name != "" {
			fields = append(fields, fb)
		}
	}
	return fields, nil
}

// Get 获取指定字段
func (m *FieldDefManager) Get(name string) (*FieldBlock, error) {
	fields, err := m.LoadAll()
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		if f.Name == name {
			return f, nil
		}
	}
	return nil, common.NewError(3001, fmt.Sprintf("field '%s' not found", name))
}

// Exists 检查字段是否存在
func (m *FieldDefManager) Exists(name string) bool {
	_, err := m.Get(name)
	return err == nil
}

// Add 添加字段
func (m *FieldDefManager) Add(fb *FieldBlock) error {
	if len(fb.Name) > common.MaxNameLen {
		return common.NewError(3002, "field name too long")
	}
	if m.Exists(fb.Name) {
		return common.NewError(3003, fmt.Sprintf("field '%s' already exists", fb.Name))
	}
	fields, _ := m.LoadAll()
	fb.Order = int32(len(fields))
	if fb.Mtime.IsZero() {
		fb.Mtime = time.Now()
	}

	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	return bf.Append(fb.Serialize())
}

// Modify 修改字段
func (m *FieldDefManager) Modify(oldName string, fb *FieldBlock) error {
	fields, err := m.LoadAll()
	if err != nil {
		return err
	}
	found := false
	for i, f := range fields {
		if f.Name == oldName {
			fields[i] = fb
			found = true
			break
		}
	}
	if !found {
		return common.NewError(3001, fmt.Sprintf("field '%s' not found", oldName))
	}

	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	bf.Truncate(0)
	for _, f := range fields {
		bf.Append(f.Serialize())
	}
	return nil
}

// Drop 删除字段
func (m *FieldDefManager) Drop(name string) error {
	fields, err := m.LoadAll()
	if err != nil {
		return err
	}
	var newFields []*FieldBlock
	found := false
	for _, f := range fields {
		if f.Name == name {
			found = true
			continue
		}
		newFields = append(newFields, f)
	}
	if !found {
		return common.NewError(3001, fmt.Sprintf("field '%s' not found", name))
	}
	// 重新排序
	for i, f := range newFields {
		f.Order = int32(i)
	}

	bf, err := storage.OpenBlockFile(m.path())
	if err != nil {
		return err
	}
	defer bf.Close()
	bf.Truncate(0)
	for _, f := range newFields {
		bf.Append(f.Serialize())
	}
	return nil
}

// ToStorageDefs 转换为存储层字段定义
func (m *FieldDefManager) ToStorageDefs() ([]storage.FieldDef, error) {
	fields, err := m.LoadAll()
	if err != nil {
		return nil, err
	}
	var defs []storage.FieldDef
	for _, f := range fields {
		defs = append(defs, storage.FieldDef{
			Name:  f.Name,
			Type:  types.DataType(f.Type),
			Param: f.Param,
		})
	}
	return defs, nil
}
