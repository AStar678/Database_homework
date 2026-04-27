package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ruanko/dbms/pkg/common"
)

// DataType 数据类型枚举
type DataType int32

const (
	INTEGER DataType = iota
	BOOL
	DOUBLE
	VARCHAR
	DATETIME
)

// TypeSize 返回类型的基础大小（不含对齐）
func TypeSize(t DataType, param int32) int {
	switch t {
	case INTEGER:
		return 4
	case BOOL:
		return 1
	case DOUBLE:
		return 8
	case VARCHAR:
		return int(param) + 1 // n+1 bytes, \0结尾
	case DATETIME:
		return 16
	default:
		return 4
	}
}

// TypeSizeAligned 返回对齐到4倍数的大小
func TypeSizeAligned(t DataType, param int32) int {
	return common.Align4(TypeSize(t, param))
}

// TypeName 返回类型名称
func TypeName(t DataType) string {
	switch t {
	case INTEGER:
		return "INTEGER"
	case BOOL:
		return "BOOL"
	case DOUBLE:
		return "DOUBLE"
	case VARCHAR:
		return "VARCHAR"
	case DATETIME:
		return "DATETIME"
	default:
		return "UNKNOWN"
	}
}

// ParseType 从字符串解析类型
func ParseType(s string) (DataType, int32, error) {
	s = strings.ToUpper(s)
	switch {
	case s == "INT" || s == "INTEGER":
		return INTEGER, 0, nil
	case s == "BOOL" || s == "BOOLEAN":
		return BOOL, 0, nil
	case s == "DOUBLE":
		return DOUBLE, 0, nil
	case strings.HasPrefix(s, "VARCHAR"):
		// VARCHAR(n)
		start := strings.Index(s, "(")
		end := strings.Index(s, ")")
		if start > 0 && end > start {
			n, err := strconv.Atoi(s[start+1 : end])
			if err != nil {
				return 0, 0, fmt.Errorf("invalid VARCHAR size")
			}
			if n < 1 || n > 255 {
				return 0, 0, fmt.Errorf("VARCHAR size must be 1-255")
			}
			return VARCHAR, int32(n), nil
		}
		return VARCHAR, 255, nil
	case s == "DATETIME":
		return DATETIME, 0, nil
	default:
		return 0, 0, fmt.Errorf("unknown type: %s", s)
	}
}

// Value 是所有数据值的接口
type Value interface {
	Type() DataType
	Serialize(param int32) []byte
	String() string
}

// IntValue INTEGER值
type IntValue struct {
	V int32
}

func (v *IntValue) Type() DataType { return INTEGER }
func (v *IntValue) Serialize(param int32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, v.V)
	return buf.Bytes()
}
func (v *IntValue) String() string { return strconv.FormatInt(int64(v.V), 10) }

// BoolValue BOOL值
type BoolValue struct {
	V bool
}

func (v *BoolValue) Type() DataType { return BOOL }
func (v *BoolValue) Serialize(param int32) []byte {
	if v.V {
		return []byte{1}
	}
	return []byte{0}
}
func (v *BoolValue) String() string {
	if v.V {
		return "true"
	}
	return "false"
}

// DoubleValue DOUBLE值
type DoubleValue struct {
	V float64
}

func (v *DoubleValue) Type() DataType { return DOUBLE }
func (v *DoubleValue) Serialize(param int32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, v.V)
	return buf.Bytes()
}
func (v *DoubleValue) String() string { return strconv.FormatFloat(v.V, 'f', -1, 64) }

// VarcharValue VARCHAR值
type VarcharValue struct {
	V string
}

func (v *VarcharValue) Type() DataType { return VARCHAR }
func (v *VarcharValue) Serialize(param int32) []byte {
	size := int(param) + 1
	b := make([]byte, size)
	copy(b, v.V)
	return b
}
func (v *VarcharValue) String() string { return v.V }

// DateTimeValue DATETIME值
type DateTimeValue struct {
	V time.Time
}

func (v *DateTimeValue) Type() DataType { return DATETIME }
func (v *DateTimeValue) Serialize(param int32) []byte {
	var buf bytes.Buffer
	common.WriteDateTime(&buf, v.V)
	return buf.Bytes()
}
func (v *DateTimeValue) String() string { return v.V.Format("2006-01-02 15:04:05") }

// NullValue 空值
type NullValue struct{}

func (v *NullValue) Type() DataType   { return INTEGER } // dummy
func (v *NullValue) Serialize(p int32) []byte { return nil }
func (v *NullValue) String() string   { return "NULL" }

// IsNull 判断是否为NULL
func IsNull(v Value) bool {
	_, ok := v.(*NullValue)
	return ok
}

// DeserializeValue 从字节反序列化值
func DeserializeValue(data []byte, offset int, t DataType, param int32) (Value, int) {
	switch t {
	case INTEGER:
		if offset+4 > len(data) {
			return &NullValue{}, 4
		}
		var v int32
		binary.Read(bytes.NewReader(data[offset:offset+4]), binary.LittleEndian, &v)
		return &IntValue{V: v}, 4
	case BOOL:
		if offset+1 > len(data) {
			return &NullValue{}, 1
		}
		return &BoolValue{V: data[offset] != 0}, 1
	case DOUBLE:
		if offset+8 > len(data) {
			return &NullValue{}, 8
		}
		var v float64
		binary.Read(bytes.NewReader(data[offset:offset+8]), binary.LittleEndian, &v)
		return &DoubleValue{V: v}, 8
	case VARCHAR:
		size := int(param) + 1
		if offset+size > len(data) {
			return &NullValue{}, common.Align4(size)
		}
		s := common.ReadFixedString(data, offset, size)
		// remove trailing \0 if any
		if idx := strings.IndexByte(s, 0); idx >= 0 {
			s = s[:idx]
		}
		return &VarcharValue{V: s}, common.Align4(size)
	case DATETIME:
		if offset+16 > len(data) {
			return &NullValue{}, 16
		}
		t := common.ReadDateTime(data, offset)
		return &DateTimeValue{V: t}, 16
	default:
		return &NullValue{}, 4
	}
}

// CompareValues 比较两个值，返回 -1, 0, 1
func CompareValues(a, b Value) int {
	if IsNull(a) && IsNull(b) {
		return 0
	}
	if IsNull(a) {
		return -1
	}
	if IsNull(b) {
		return 1
	}
	if a.Type() != b.Type() {
		// 尝试数值比较
		switch av := a.(type) {
		case *IntValue:
			switch bv := b.(type) {
			case *DoubleValue:
				if float64(av.V) < bv.V {
					return -1
				} else if float64(av.V) > bv.V {
					return 1
				}
				return 0
			}
		case *DoubleValue:
			switch bv := b.(type) {
			case *IntValue:
				if av.V < float64(bv.V) {
					return -1
				} else if av.V > float64(bv.V) {
					return 1
				}
				return 0
			}
		}
		return 0
	}
	switch av := a.(type) {
	case *IntValue:
		bv := b.(*IntValue)
		if av.V < bv.V {
			return -1
		} else if av.V > bv.V {
			return 1
		}
		return 0
	case *BoolValue:
		bv := b.(*BoolValue)
		if !av.V && bv.V {
			return -1
		} else if av.V && !bv.V {
			return 1
		}
		return 0
	case *DoubleValue:
		bv := b.(*DoubleValue)
		if av.V < bv.V {
			return -1
		} else if av.V > bv.V {
			return 1
		}
		return 0
	case *VarcharValue:
		bv := b.(*VarcharValue)
		if av.V < bv.V {
			return -1
		} else if av.V > bv.V {
			return 1
		}
		return 0
	case *DateTimeValue:
		bv := b.(*DateTimeValue)
		if av.V.Before(bv.V) {
			return -1
		} else if av.V.After(bv.V) {
			return 1
		}
		return 0
	}
	return 0
}

// ParseValue 从字符串解析为Value
func ParseValue(s string, t DataType, param int32) (Value, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "NULL") {
		return &NullValue{}, nil
	}
	switch t {
	case INTEGER:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return &IntValue{V: int32(v)}, nil
	case BOOL:
		if strings.EqualFold(s, "true") || s == "1" {
			return &BoolValue{V: true}, nil
		}
		return &BoolValue{V: false}, nil
	case DOUBLE:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return &DoubleValue{V: v}, nil
	case VARCHAR:
		// 去除可能的引号
		if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
			(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
			s = s[1 : len(s)-1]
		}
		if len(s) > int(param) {
			return nil, fmt.Errorf("string too long for VARCHAR(%d)", param)
		}
		return &VarcharValue{V: s}, nil
	case DATETIME:
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
			if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
				return &DateTimeValue{V: t}, nil
			}
		}
		return nil, fmt.Errorf("invalid datetime format: %s", s)
	default:
		return nil, fmt.Errorf("unknown type")
	}
}
