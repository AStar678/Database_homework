package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// WriteFixedString 写入固定长度的字符串，不足补零
func WriteFixedString(buf *bytes.Buffer, s string, fixedLen int) {
	b := make([]byte, fixedLen)
	copy(b, s)
	buf.Write(b)
}

// ReadFixedString 读取固定长度的字符串，去除尾部零字节
func ReadFixedString(data []byte, offset int, fixedLen int) string {
	if offset+fixedLen > len(data) {
		return ""
	}
	b := data[offset : offset+fixedLen]
	idx := bytes.IndexByte(b, 0)
	if idx < 0 {
		return string(b)
	}
	return string(b[:idx])
}

// WriteBool 写入1字节布尔值
func WriteBool(buf *bytes.Buffer, v bool) {
	if v {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

// ReadBool 读取1字节布尔值
func ReadBool(data []byte, offset int) bool {
	if offset >= len(data) {
		return false
	}
	return data[offset] != 0
}

// WriteInt 写入4字节整数（小端）
func WriteInt(buf *bytes.Buffer, v int32) {
	binary.Write(buf, binary.LittleEndian, v)
}

// ReadInt 读取4字节整数（小端）
func ReadInt(data []byte, offset int) int32 {
	if offset+4 > len(data) {
		return 0
	}
	var v int32
	binary.Read(bytes.NewReader(data[offset:offset+4]), binary.LittleEndian, &v)
	return v
}

// WriteDouble 写入8字节浮点数（小端）
func WriteDouble(buf *bytes.Buffer, v float64) {
	binary.Write(buf, binary.LittleEndian, v)
}

// ReadDouble 读取8字节浮点数（小端）
func ReadDouble(data []byte, offset int) float64 {
	if offset+8 > len(data) {
		return 0
	}
	var v float64
	binary.Read(bytes.NewReader(data[offset:offset+8]), binary.LittleEndian, &v)
	return v
}

// WriteDateTime 写入16字节日期时间
func WriteDateTime(buf *bytes.Buffer, t time.Time) {
	// 格式: year(2) month(1) day(1) hour(1) minute(1) second(1) millisecond(2) pad(7)
	year := int16(t.Year())
	binary.Write(buf, binary.LittleEndian, year)
	buf.WriteByte(byte(t.Month()))
	buf.WriteByte(byte(t.Day()))
	buf.WriteByte(byte(t.Hour()))
	buf.WriteByte(byte(t.Minute()))
	buf.WriteByte(byte(t.Second()))
	ms := int16(0)
	binary.Write(buf, binary.LittleEndian, ms)
	buf.Write(make([]byte, 7)) // padding to 16 bytes
}

// ReadDateTime 读取16字节日期时间
func ReadDateTime(data []byte, offset int) time.Time {
	if offset+16 > len(data) {
		return time.Time{}
	}
	reader := bytes.NewReader(data[offset : offset+16])
	var year int16
	binary.Read(reader, binary.LittleEndian, &year)
	month, _ := reader.ReadByte()
	day, _ := reader.ReadByte()
	hour, _ := reader.ReadByte()
	minute, _ := reader.ReadByte()
	second, _ := reader.ReadByte()
	// skip millisecond + padding
	return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), 0, time.Local)
}

// NowDateTimeBytes 返回当前时间的16字节编码
func NowDateTimeBytes() []byte {
	var buf bytes.Buffer
	WriteDateTime(&buf, time.Now())
	return buf.Bytes()
}

// EnsureDir 确保目录存在
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// FileExists 检查文件是否存在
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// RemoveFile 删除文件
func RemoveFile(path string) error {
	return os.Remove(path)
}

// CopyDir 复制目录（用于备份）
func CopyDir(src, dst string) error {
	return os.CopyFS(dst, os.DirFS(src))
}

// DBError 数据库错误类型
type DBError struct {
	Code    int
	Message string
}

func (e *DBError) Error() string {
	return fmt.Sprintf("[ERR%d] %s", e.Code, e.Message)
}

// NewError 创建错误
func NewError(code int, msg string) error {
	return &DBError{Code: code, Message: msg}
}
