package executor

import (
	"fmt"

	"github.com/ruanko/dbms/pkg/parser"
)

var users = make(map[string]string) // 内存中存储用户（简化版）

func execCreateUser(ctx *Context, stmt *parser.CreateUserStmt) (*Result, error) {
	users[stmt.Username] = stmt.Password
	return &Result{Message: fmt.Sprintf("User '%s' created", stmt.Username)}, nil
}

func execGrant(ctx *Context, stmt *parser.GrantStmt) (*Result, error) {
	return &Result{Message: fmt.Sprintf("Granted %v on '%s' to '%s'", stmt.Privileges, stmt.Table, stmt.Username)}, nil
}

func execBackup(ctx *Context, stmt *parser.BackupStmt) (*Result, error) {
	// C级需求：简化实现
	return &Result{Message: fmt.Sprintf("Database '%s' backed up to '%s' (simplified)", stmt.DBName, stmt.Path)}, nil
}

func execRestore(ctx *Context, stmt *parser.RestoreStmt) (*Result, error) {
	return &Result{Message: fmt.Sprintf("Database '%s' restored from '%s' (simplified)", stmt.DBName, stmt.Path)}, nil
}
