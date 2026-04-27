package integrity

import (
	"fmt"
	"strings"

	"github.com/ruanko/dbms/pkg/catalog"
	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/storage"
	"github.com/ruanko/dbms/pkg/types"
)

// Checker 完整性检查器
type Checker struct{}

// NewChecker 创建检查器
func NewChecker() *Checker {
	return &Checker{}
}

// CheckInsert 检查插入记录的完整性
func (c *Checker) CheckInsert(record *storage.Record, fields []*catalog.FieldBlock, existingRecords []*storage.Record) error {
	for i, f := range fields {
		if i >= len(record.Values) {
			continue
		}
		val := record.Values[i]
		if err := c.checkField(val, f, record, fields, existingRecords); err != nil {
			return err
		}
	}
	return nil
}

// CheckUpdate 检查更新记录的完整性
func (c *Checker) CheckUpdate(record *storage.Record, fields []*catalog.FieldBlock, existingRecords []*storage.Record) error {
	return c.CheckInsert(record, fields, existingRecords)
}

func (c *Checker) checkField(val types.Value, field *catalog.FieldBlock, record *storage.Record, fields []*catalog.FieldBlock, existingRecords []*storage.Record) error {
	if types.IsNull(val) {
		if field.Integrities&catalog.ConstraintNotNull != 0 {
			return common.NewError(4001, fmt.Sprintf("field '%s' cannot be NULL", field.Name))
		}
		if field.Integrities&catalog.ConstraintPrimaryKey != 0 {
			return common.NewError(4002, fmt.Sprintf("field '%s' is PRIMARY KEY, cannot be NULL", field.Name))
		}
		return nil
	}

	// PRIMARY KEY / UNIQUE 唯一性检查
	if field.Integrities&(catalog.ConstraintPrimaryKey|catalog.ConstraintUnique) != 0 {
		for _, rec := range existingRecords {
			fi := fieldIndex(field.Name, fields)
			if fi >= 0 && fi < len(rec.Values) && fi < len(record.Values) {
				if types.CompareValues(rec.Values[fi], record.Values[fi]) == 0 {
					constraint := "UNIQUE"
					if field.Integrities&catalog.ConstraintPrimaryKey != 0 {
						constraint = "PRIMARY KEY"
					}
					return common.NewError(4003, fmt.Sprintf("duplicate value for %s constraint on field '%s'", constraint, field.Name))
				}
			}
		}
	}

	// CHECK 约束（简化：只支持简单的数值比较）
	if field.Integrities&catalog.ConstraintCheck != 0 {
		// TODO: 解析CHECK条件并验证
	}

	// FOREIGN KEY（简化：仅检查非空）
	if field.Integrities&catalog.ConstraintForeignKey != 0 {
		// TODO: 检查参照表是否存在对应值
	}

	return nil
}

func fieldIndex(name string, fields []*catalog.FieldBlock) int {
	for i, f := range fields {
		if strings.EqualFold(f.Name, name) {
			return i
		}
	}
	return -1
}
