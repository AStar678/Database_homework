package executor

import (
	"fmt"

	"github.com/ruanko/dbms/pkg/catalog"
	"github.com/ruanko/dbms/pkg/parser"
	"github.com/ruanko/dbms/pkg/types"
)

// Result 执行结果
type Result struct {
	Message    string
	Rows       [][]string
	Columns    []string
	RowsAffected int
}

// Context 执行上下文
type Context struct {
	Catalog      *catalog.Manager
	CurrentDB    string
	Transaction  interface{} // 简化：可扩展
	User         string
}

// NewContext 创建执行上下文
func NewContext() *Context {
	return &Context{
		Catalog: catalog.NewManager(),
	}
}

// Execute 执行SQL语句
func Execute(ctx *Context, stmt parser.Stmt) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		return execCreateDatabase(ctx, s)
	case *parser.DropDatabaseStmt:
		return execDropDatabase(ctx, s)
	case *parser.UseDatabaseStmt:
		return execUseDatabase(ctx, s)
	case *parser.ShowDatabasesStmt:
		return execShowDatabases(ctx, s)
	case *parser.ShowTablesStmt:
		return execShowTables(ctx, s)
	case *parser.CreateTableStmt:
		return execCreateTable(ctx, s)
	case *parser.DropTableStmt:
		return execDropTable(ctx, s)
	case *parser.AlterTableStmt:
		return execAlterTable(ctx, s)
	case *parser.InsertStmt:
		return execInsert(ctx, s)
	case *parser.UpdateStmt:
		return execUpdate(ctx, s)
	case *parser.DeleteStmt:
		return execDelete(ctx, s)
	case *parser.SelectStmt:
		return execSelect(ctx, s)
	case *parser.CreateIndexStmt:
		return execCreateIndex(ctx, s)
	case *parser.DropIndexStmt:
		return execDropIndex(ctx, s)
	case *parser.CreateUserStmt:
		return execCreateUser(ctx, s)
	case *parser.GrantStmt:
		return execGrant(ctx, s)
	case *parser.BackupStmt:
		return execBackup(ctx, s)
	case *parser.RestoreStmt:
		return execRestore(ctx, s)
	case *parser.BeginStmt:
		return &Result{Message: "Transaction started (simplified)"}, nil
	case *parser.CommitStmt:
		return &Result{Message: "Transaction committed (simplified)"}, nil
	case *parser.RollbackStmt:
		return &Result{Message: "Transaction rolled back (simplified)"}, nil
	default:
		return nil, fmt.Errorf("unsupported statement: %T", stmt)
	}
}

func ensureDB(ctx *Context) error {
	if ctx.CurrentDB == "" {
		return fmt.Errorf("no database selected")
	}
	return nil
}

func valueToString(v types.Value) string {
	if types.IsNull(v) {
		return "NULL"
	}
	return v.String()
}
