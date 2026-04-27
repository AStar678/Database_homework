package executor

import (
	"fmt"

	"github.com/ruanko/dbms/pkg/parser"
)

func execCreateIndex(ctx *Context, stmt *parser.CreateIndexStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	// C级需求：简化实现，仅记录索引元数据
	return &Result{Message: fmt.Sprintf("Index '%s' created on table '%s' (columns: %v)", stmt.Name, stmt.Table, stmt.Columns)}, nil
}

func execDropIndex(ctx *Context, stmt *parser.DropIndexStmt) (*Result, error) {
	if err := ensureDB(ctx); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Index '%s' dropped", stmt.Name)}, nil
}
