package storage

import (
	"bytes"

	"github.com/ruanko/dbms/pkg/types"
)

// Record 表示一条记录，包含字段值列表
type Record struct {
	Values  []types.Value
	Deleted bool
}

// RecordFile 记录文件管理
type RecordFile struct {
	bf *BlockFile
}

// OpenRecordFile 打开记录文件
func OpenRecordFile(path string) (*RecordFile, error) {
	bf, err := OpenBlockFile(path)
	if err != nil {
		return nil, err
	}
	return &RecordFile{bf: bf}, nil
}

// Close 关闭记录文件
func (rf *RecordFile) Close() error {
	return rf.bf.Close()
}

// RecordSize 计算一条记录的大小（基于字段定义），包含4字节删除标记
func RecordSize(fields []FieldDef) int {
	size := 4 // deleted flag aligned to 4 bytes
	for _, f := range fields {
		size += types.TypeSizeAligned(f.Type, f.Param)
	}
	return size
}

// FieldDef 字段定义（用于记录序列化）
type FieldDef struct {
	Name  string
	Type  types.DataType
	Param int32
}

// SerializeRecord 将记录序列化为字节
func SerializeRecord(record *Record, fields []FieldDef) []byte {
	var buf bytes.Buffer
	// 删除标记 (4 bytes)
	if record.Deleted {
		buf.Write([]byte{1, 0, 0, 0})
	} else {
		buf.Write([]byte{0, 0, 0, 0})
	}
	for i, val := range record.Values {
		if i < len(fields) {
			data := val.Serialize(fields[i].Param)
			// 确保写入对齐大小
			aligned := types.TypeSizeAligned(fields[i].Type, fields[i].Param)
			if len(data) < aligned {
				data = append(data, make([]byte, aligned-len(data))...)
			}
			buf.Write(data)
		}
	}
	return buf.Bytes()
}

// DeserializeRecord 从字节反序列化记录
func DeserializeRecord(data []byte, fields []FieldDef) *Record {
	record := &Record{Values: make([]types.Value, len(fields))}
	offset := 0
	if len(data) >= 4 {
		record.Deleted = data[0] != 0
		offset = 4
	}
	for i, f := range fields {
		if offset >= len(data) {
			break
		}
		val, consumed := types.DeserializeValue(data, offset, f.Type, f.Param)
		record.Values[i] = val
		offset += consumed
	}
	return record
}

// Insert 插入记录，返回偏移量
func (rf *RecordFile) Insert(record *Record, fields []FieldDef) (int64, error) {
	data := SerializeRecord(record, fields)
	offset, err := rf.bf.Size()
	if err != nil {
		return 0, err
	}
	return offset, rf.bf.Append(data)
}

// ReadAt 读取指定偏移的记录
func (rf *RecordFile) ReadAt(offset int64, fields []FieldDef) (*Record, error) {
	size := RecordSize(fields)
	data, err := rf.bf.ReadAt(offset, size)
	if err != nil {
		return nil, err
	}
	return DeserializeRecord(data, fields), nil
}

// UpdateAt 更新指定偏移的记录
func (rf *RecordFile) UpdateAt(offset int64, record *Record, fields []FieldDef) error {
	data := SerializeRecord(record, fields)
	return rf.bf.WriteAt(offset, data)
}

// ScanAll 扫描所有记录，返回偏移量和记录的列表
func (rf *RecordFile) ScanAll(fields []FieldDef) ([]int64, []*Record, error) {
	data, err := rf.bf.ReadAllBlocks()
	if err != nil {
		return nil, nil, err
	}
	recordSize := RecordSize(fields)
	if recordSize == 0 {
		return nil, nil, nil
	}
	var offsets []int64
	var records []*Record
	for offset := 0; offset+recordSize <= len(data); offset += recordSize {
		rec := DeserializeRecord(data[offset:offset+recordSize], fields)
		offsets = append(offsets, int64(offset))
		records = append(records, rec)
	}
	return offsets, records, nil
}
