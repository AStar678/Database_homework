package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/ruanko/dbms/pkg/catalog"
	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/parser"
	"github.com/ruanko/dbms/pkg/storage"
	"github.com/ruanko/dbms/pkg/types"
)

func execInsert(ctx *Context, stmt *parser.InsertStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	table, err := ctx.Catalog.GetTable(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fields, err := ctx.Catalog.GetFields(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fieldDefs, err := ctx.Catalog.GetFieldDefs(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}

	// 获取字段名列表
	var fieldNames []string
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}

	// 确定插入顺序
	var insertOrder []int // 字段索引 -> values中的索引，-1表示使用默认值/NULL
	if len(stmt.Columns) == 0 {
		// 按表定义顺序
		for i := 0; i < len(fields); i++ {
			insertOrder = append(insertOrder, i)
		}
	} else {
		// 建立列名到值的映射
		colMap := make(map[string]int)
		for i, col := range stmt.Columns {
			colMap[strings.ToUpper(col)] = i
		}
		for _, f := range fields {
			if idx, ok := colMap[strings.ToUpper(f.Name)]; ok {
				insertOrder = append(insertOrder, idx)
			} else {
				insertOrder = append(insertOrder, -1)
			}
		}
	}

	rf, err := ctx.Catalog.OpenRecordFile(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	inserted := 0
	for _, rowValues := range stmt.Values {
		record := &storage.Record{Values: make([]types.Value, len(fields))}
		for i, f := range fields {
			valIdx := insertOrder[i]
			if valIdx >= 0 && valIdx < len(rowValues) {
				record.Values[i] = rowValues[valIdx]
			} else {
				// 默认值或NULL
				record.Values[i] = &types.NullValue{}
				// 如果有DEFAULT约束
				if f.Integrities&catalog.ConstraintDefault != 0 {
					// 查找DEFAULT值（简化：从约束信息中解析）
					// 实际应该从tic文件读取，这里简化
				}
				if f.Integrities&catalog.ConstraintIdentity != 0 {
					record.Values[i] = &types.IntValue{V: table.RecordNum + int32(inserted) + 1}
				}
			}
		}

		// 完整性检查
		if err := checkInsert(record, fields, rf); err != nil {
			return nil, err
		}

		_, err = rf.Insert(record, fieldDefs)
		if err != nil {
			return nil, err
		}
		inserted++
	}

	// 更新记录数
	ctx.Catalog.GetTableMeta(ctx.CurrentDB)
	tm := catalog.NewTableMetaManager(ctx.CurrentDB)
	tm.Update(stmt.Table, func(tb *catalog.TableBlock) {
		tb.RecordNum += int32(inserted)
		tb.Mtime = int32(time.Now().Unix())
	})

	return &Result{Message: fmt.Sprintf("%d row(s) inserted", inserted), RowsAffected: inserted}, nil
}

func execUpdate(ctx *Context, stmt *parser.UpdateStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	table, err := ctx.Catalog.GetTable(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	if table.RecordNum == 0 {
		return &Result{Message: "0 row(s) updated", RowsAffected: 0}, nil
	}
	fields, err := ctx.Catalog.GetFields(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fieldDefs, err := ctx.Catalog.GetFieldDefs(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}

	var fieldNames []string
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}

	rf, err := ctx.Catalog.OpenRecordFile(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	offsets, records, err := rf.ScanAll(fieldDefs)
	if err != nil {
		return nil, err
	}

	updated := 0
	for i, rec := range records {
		match, err := MatchWhere(stmt.Where, rec, fieldNames)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		for _, set := range stmt.Sets {
			for fi, name := range fieldNames {
				if strings.EqualFold(name, set.Column) {
					val, err := EvalExpr(set.Value, rec, fieldNames)
					if err != nil {
						return nil, err
					}
					rec.Values[fi] = val
					break
				}
			}
		}
		if err := checkUpdate(rec, fields, rf); err != nil {
			return nil, err
		}
		rf.UpdateAt(offsets[i], rec, fieldDefs)
		updated++
	}

	return &Result{Message: fmt.Sprintf("%d row(s) updated", updated), RowsAffected: updated}, nil
}

func execDelete(ctx *Context, stmt *parser.DeleteStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	table, err := ctx.Catalog.GetTable(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	if table.RecordNum == 0 {
		return &Result{Message: "0 row(s) deleted", RowsAffected: 0}, nil
	}
	fields, err := ctx.Catalog.GetFields(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fieldDefs, err := ctx.Catalog.GetFieldDefs(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}

	var fieldNames []string
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}

	rf, err := ctx.Catalog.OpenRecordFile(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	offsets, records, err := rf.ScanAll(fieldDefs)
	if err != nil {
		return nil, err
	}

	deleted := 0
	// 标记删除
	nullRecord := &storage.Record{Values: make([]types.Value, len(fields)), Deleted: true}
	for i := range nullRecord.Values {
		nullRecord.Values[i] = &types.NullValue{}
	}

	for i, rec := range records {
		if rec.Deleted {
			continue
		}
		match, err := MatchWhere(stmt.Where, rec, fieldNames)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		rf.UpdateAt(offsets[i], nullRecord, fieldDefs)
		deleted++
	}

	// 更新记录数
	tm := catalog.NewTableMetaManager(ctx.CurrentDB)
	tm.Update(stmt.Table, func(tb *catalog.TableBlock) {
		tb.RecordNum -= int32(deleted)
		if tb.RecordNum < 0 {
			tb.RecordNum = 0
		}
	})

	return &Result{Message: fmt.Sprintf("%d row(s) deleted", deleted), RowsAffected: deleted}, nil
}

func execSelect(ctx *Context, stmt *parser.SelectStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	_, err := ctx.Catalog.GetTable(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fields, err := ctx.Catalog.GetFields(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	fieldDefs, err := ctx.Catalog.GetFieldDefs(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}

	var fieldNames []string
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}

	// 确定输出列
	var outputCols []string
	var outputIndices []int
	if len(stmt.Columns) == 0 {
		// SELECT *
		outputCols = fieldNames
		for i := range fields {
			outputIndices = append(outputIndices, i)
		}
	} else {
		for _, col := range stmt.Columns {
			found := false
			for fi, name := range fieldNames {
				if strings.EqualFold(name, col) {
					outputCols = append(outputCols, name)
					outputIndices = append(outputIndices, fi)
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column '%s' not found", col)
			}
		}
	}

	rf, err := ctx.Catalog.OpenRecordFile(ctx.CurrentDB, stmt.Table)
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	_, records, err := rf.ScanAll(fieldDefs)
	if err != nil {
		return nil, err
	}

	var rows [][]string
	matched := 0
	for _, rec := range records {
		// 跳过已删除记录
		if rec.Deleted {
			continue
		}

		match, err := MatchWhere(stmt.Where, rec, fieldNames)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		matched++
		var row []string
		for _, idx := range outputIndices {
			if idx < len(rec.Values) {
				row = append(row, valueToString(rec.Values[idx]))
			} else {
				row = append(row, "NULL")
			}
		}
		rows = append(rows, row)
	}

	return &Result{
		Message:    fmt.Sprintf("%d row(s) returned", matched),
		Columns:    outputCols,
		Rows:       rows,
		RowsAffected: matched,
	}, nil
}

// checkInsert 插入前完整性检查（简化版）
func checkInsert(record *storage.Record, fields []*catalog.FieldBlock, rf *storage.RecordFile) error {
	for i, f := range fields {
		if i >= len(record.Values) {
			continue
		}
		val := record.Values[i]
		if types.IsNull(val) {
			if f.Integrities&catalog.ConstraintNotNull != 0 || f.Integrities&catalog.ConstraintPrimaryKey != 0 {
				return common.NewError(4001, fmt.Sprintf("field '%s' cannot be NULL", f.Name))
			}
		}
		// 主键/唯一性检查（简化版）
		if f.Integrities&(catalog.ConstraintPrimaryKey|catalog.ConstraintUnique) != 0 && !types.IsNull(val) {
			// TODO: 扫描现有记录检查唯一性
			_ = rf
		}
	}
	return nil
}

// checkUpdate 更新前完整性检查（简化版）
func checkUpdate(record *storage.Record, fields []*catalog.FieldBlock, rf *storage.RecordFile) error {
	return checkInsert(record, fields, rf)
}
