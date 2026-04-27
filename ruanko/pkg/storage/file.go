package storage

import (
	"os"
)

// BlockFile 封装二进制块文件操作
type BlockFile struct {
	path string
	file *os.File
}

// OpenBlockFile 打开或创建块文件
func OpenBlockFile(path string) (*BlockFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &BlockFile{path: path, file: f}, nil
}

// Close 关闭文件
func (bf *BlockFile) Close() error {
	if bf.file != nil {
		return bf.file.Close()
	}
	return nil
}

// ReadAt 从指定偏移读取数据
func (bf *BlockFile) ReadAt(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := bf.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// WriteAt 在指定偏移写入数据
func (bf *BlockFile) WriteAt(offset int64, data []byte) error {
	_, err := bf.file.WriteAt(data, offset)
	return err
}

// Append 追加数据到文件末尾
func (bf *BlockFile) Append(data []byte) error {
	_, err := bf.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	_, err = bf.file.Write(data)
	return err
}

// Size 返回文件大小
func (bf *BlockFile) Size() (int64, error) {
	info, err := bf.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Truncate 截断文件到指定大小
func (bf *BlockFile) Truncate(size int64) error {
	return bf.file.Truncate(size)
}

// DeleteBlockFile 删除块文件
func DeleteBlockFile(path string) error {
	return os.Remove(path)
}

// FileExists 检查文件是否存在
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ReadAllBlocks 读取文件所有数据
func (bf *BlockFile) ReadAllBlocks() ([]byte, error) {
	size, err := bf.Size()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return []byte{}, nil
	}
	return bf.ReadAt(0, int(size))
}
