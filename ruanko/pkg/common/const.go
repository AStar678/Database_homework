package common

import (
	"os"
	"path/filepath"
)

// DBMSRoot 返回DBMS安装根目录，默认使用当前工作目录下的 dbms_root
func DBMSRoot() string {
	if r := os.Getenv("DBMS_ROOT"); r != "" {
		return r
	}
	cwd, _ := os.Getwd()
	return filepath.Join(cwd, "dbms_root")
}

// DataDir 返回数据目录 [DBMS_ROOT]/data
func DataDir() string {
	return filepath.Join(DBMSRoot(), "data")
}

// DBDir 返回指定数据库的数据目录
func DBDir(dbName string) string {
	return filepath.Join(DataDir(), dbName)
}

// DBMetaFile 返回数据库描述文件路径 ruanko.db
func DBMetaFile() string {
	return filepath.Join(DBMSRoot(), "ruanko.db")
}

// TableMetaFile 返回表描述文件路径 [dbname].tb
func TableMetaFile(dbName string) string {
	return filepath.Join(DBDir(dbName), dbName+".tb")
}

// TableDefFile 返回表定义文件路径 [table].tdf
func TableDefFile(dbName, tableName string) string {
	return filepath.Join(DBDir(dbName), tableName+".tdf")
}

// TableRecordFile 返回记录文件路径 [table].trd
func TableRecordFile(dbName, tableName string) string {
	return filepath.Join(DBDir(dbName), tableName+".trd")
}

// TableIntegrityFile 返回完整性描述文件路径 [table].tic
func TableIntegrityFile(dbName, tableName string) string {
	return filepath.Join(DBDir(dbName), tableName+".tic")
}

// TableIndexFile 返回索引描述文件路径 [table].tid
func TableIndexFile(dbName, tableName string) string {
	return filepath.Join(DBDir(dbName), tableName+".tid")
}

// IndexDataFile 返回索引数据文件路径 [indexName].ix
func IndexDataFile(dbName, indexName string) string {
	return filepath.Join(DBDir(dbName), indexName+".ix")
}

// LogFile 返回日志文件路径 [dbname].log
func LogFile(dbName string) string {
	return filepath.Join(DBDir(dbName), dbName+".log")
}

// Align4 将大小对齐到4的倍数
func Align4(n int) int {
	if n%4 == 0 {
		return n
	}
	return n + (4 - n%4)
}

// MaxNameLen 最大名称长度
const MaxNameLen = 128

// MaxPathLen 最大路径长度
const MaxPathLen = 256

// SystemDBName 系统数据库名称
const SystemDBName = "Ruanko"
