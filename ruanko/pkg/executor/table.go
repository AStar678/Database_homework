package executor

import (
	"fmt"
	"time"

	"github.com/ruanko/dbms/pkg/catalog"
	"github.com/ruanko/dbms/pkg/parser"
	"github.com/ruanko/dbms/pkg/types"
)

func execShowTables(ctx *Context, stmt *parser.ShowTablesStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	tables, err := ctx.Catalog.ListTables(ctx.CurrentDB)
	if err != nil {
		return nil, err
	}
	var rows [][]string
	for _, t := range tables {
		rows = append(rows, []string{t.Name, fmt.Sprintf("%d", t.RecordNum), fmt.Sprintf("%d", t.FieldNum), t.Crtime.Format("2006-01-02 15:04:05")})
	}
	return &Result{
		Message: fmt.Sprintf("%d table(s) found", len(tables)),
		Columns: []string{"Name", "Records", "Fields", "Created"},
		Rows:    rows,
	}, nil
}

func execCreateTable(ctx *Context, stmt *parser.CreateTableStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	var fields []*catalog.FieldBlock
	for i, col := range stmt.Columns {
		dt, param, err := types.ParseType(col.Type)
		if err != nil {
			return nil, err
		}
		if col.Param > 0 {
			param = int32(col.Param)
		}
		integrity := 0
		for _, c := range col.Constraints {
			switch c.Type {
			case "NOT_NULL":
				integrity |= catalog.ConstraintNotNull
			case "PRIMARY_KEY":
				integrity |= catalog.ConstraintPrimaryKey
			case "UNIQUE":
				integrity |= catalog.ConstraintUnique
			case "DEFAULT":
				integrity |= catalog.ConstraintDefault
			case "IDENTITY":
				integrity |= catalog.ConstraintIdentity
			case "FOREIGN_KEY":
				integrity |= catalog.ConstraintForeignKey
			case "CHECK":
				integrity |= catalog.ConstraintCheck
			}
		}
		fb := &catalog.FieldBlock{
			Order:       int32(i),
			Name:        col.Name,
			Type:        int32(dt),
			Param:       param,
			Mtime:       time.Now(),
			Integrities: int32(integrity),
		}
		fields = append(fields, fb)
	}
	if err := ctx.Catalog.CreateTable(ctx.CurrentDB, stmt.Name, fields); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table '%s' created", stmt.Name)}, nil
}

func execDropTable(ctx *Context, stmt *parser.DropTableStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Catalog.DropTable(ctx.CurrentDB, stmt.Name); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table '%s' dropped", stmt.Name)}, nil
}

func execAlterTable(ctx *Context, stmt *parser.AlterTableStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	switch stmt.Action {
	case parser.AlterAddColumn:
		col := stmt.ColumnDef
		dt, param, err := types.ParseType(col.Type)
		if err != nil {
			return nil, err
		}
		if col.Param > 0 {
			param = int32(col.Param)
		}
		integrity := 0
		for _, c := range col.Constraints {
			switch c.Type {
			case "NOT_NULL":
				integrity |= catalog.ConstraintNotNull
			case "PRIMARY_KEY":
				integrity |= catalog.ConstraintPrimaryKey
			case "UNIQUE":
				integrity |= catalog.ConstraintUnique
			case "DEFAULT":
				integrity |= catalog.ConstraintDefault
			case "IDENTITY":
				integrity |= catalog.ConstraintIdentity
			}
		}
		fb := &catalog.FieldBlock{
			Name:        col.Name,
			Type:        int32(dt),
			Param:       param,
			Mtime:       time.Now(),
			Integrities: int32(integrity),
		}
		if err := ctx.Catalog.AddField(ctx.CurrentDB, stmt.Name, fb); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("Column '%s' added to table '%s'", col.Name, stmt.Name)}, nil
	case parser.AlterModifyColumn:
		col := stmt.ColumnDef
		dt, param, err := types.ParseType(col.Type)
		if err != nil {
			return nil, err
		}
		if col.Param > 0 {
			param = int32(col.Param)
		}
		integrity := 0
		for _, c := range col.Constraints {
			switch c.Type {
			case "NOT_NULL":
				integrity |= catalog.ConstraintNotNull
			case "PRIMARY_KEY":
				integrity |= catalog.ConstraintPrimaryKey
			case "UNIQUE":
				integrity |= catalog.ConstraintUnique
			case "DEFAULT":
				integrity |= catalog.ConstraintDefault
			case "IDENTITY":
				integrity |= catalog.ConstraintIdentity
			}
		}
		// 获取旧字段的order
		oldField, err := ctx.Catalog.GetField(ctx.CurrentDB, stmt.Name, col.Name)
		if err != nil {
			return nil, err
		}
		fb := &catalog.FieldBlock{
			Order:       oldField.Order,
			Name:        col.Name,
			Type:        int32(dt),
			Param:       param,
			Mtime:       time.Now(),
			Integrities: int32(integrity),
		}
		if err := ctx.Catalog.ModifyField(ctx.CurrentDB, stmt.Name, col.Name, fb); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("Column '%s' modified in table '%s'", col.Name, stmt.Name)}, nil
	case parser.AlterDropColumn:
		if err := ctx.Catalog.DropField(ctx.CurrentDB, stmt.Name, stmt.ColumnName); err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("Column '%s' dropped from table '%s'", stmt.ColumnName, stmt.Name)}, nil
	}
	return nil, fmt.Errorf("unknown alter action")
}
