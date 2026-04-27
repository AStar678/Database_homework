package parser

import (
	"github.com/ruanko/dbms/pkg/types"
)

// Stmt SQL语句接口
type Stmt interface {
	stmtNode()
	String() string
}

// Expr 表达式接口
type Expr interface {
	exprNode()
	String() string
}

// --- DDL Statements ---

// CreateDatabaseStmt CREATE DATABASE
type CreateDatabaseStmt struct {
	Name string
}

func (s *CreateDatabaseStmt) stmtNode() {}
func (s *CreateDatabaseStmt) String() string { return "CREATE DATABASE " + s.Name }

// DropDatabaseStmt DROP DATABASE
type DropDatabaseStmt struct {
	Name string
}

func (s *DropDatabaseStmt) stmtNode() {}
func (s *DropDatabaseStmt) String() string { return "DROP DATABASE " + s.Name }

// UseDatabaseStmt USE DATABASE
type UseDatabaseStmt struct {
	Name string
}

func (s *UseDatabaseStmt) stmtNode() {}
func (s *UseDatabaseStmt) String() string { return "USE " + s.Name }

// ShowDatabasesStmt SHOW DATABASES
type ShowDatabasesStmt struct{}

func (s *ShowDatabasesStmt) stmtNode() {}
func (s *ShowDatabasesStmt) String() string { return "SHOW DATABASES" }

// ShowTablesStmt SHOW TABLES
type ShowTablesStmt struct{}

func (s *ShowTablesStmt) stmtNode() {}
func (s *ShowTablesStmt) String() string { return "SHOW TABLES" }

// ColumnDef 列定义
type ColumnDef struct {
	Name        string
	Type        string
	Param       int
	Constraints []Constraint
}

// Constraint 约束定义
type Constraint struct {
	Type  string // NOT_NULL, PRIMARY_KEY, UNIQUE, DEFAULT, IDENTITY, FOREIGN_KEY, CHECK
	Value string // DEFAULT值或CHECK条件等
}

// CreateTableStmt CREATE TABLE
type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
}

func (s *CreateTableStmt) stmtNode() {}
func (s *CreateTableStmt) String() string { return "CREATE TABLE " + s.Name }

// DropTableStmt DROP TABLE
type DropTableStmt struct {
	Name string
}

func (s *DropTableStmt) stmtNode() {}
func (s *DropTableStmt) String() string { return "DROP TABLE " + s.Name }

// AlterTableAction ALTER TABLE动作类型
type AlterTableAction int

const (
	AlterAddColumn AlterTableAction = iota
	AlterModifyColumn
	AlterDropColumn
)

// AlterTableStmt ALTER TABLE
type AlterTableStmt struct {
	Name       string
	Action     AlterTableAction
	ColumnDef  *ColumnDef
	ColumnName string // for DROP/MODIFY
}

func (s *AlterTableStmt) stmtNode() {}
func (s *AlterTableStmt) String() string { return "ALTER TABLE " + s.Name }

// --- DML Statements ---

// InsertStmt INSERT INTO
type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]types.Value // 支持多行
}

func (s *InsertStmt) stmtNode() {}
func (s *InsertStmt) String() string { return "INSERT INTO " + s.Table }

// SetClause SET子句
type SetClause struct {
	Column string
	Value  Expr
}

// UpdateStmt UPDATE
type UpdateStmt struct {
	Table string
	Sets  []SetClause
	Where Expr
}

func (s *UpdateStmt) stmtNode() {}
func (s *UpdateStmt) String() string { return "UPDATE " + s.Table }

// DeleteStmt DELETE
type DeleteStmt struct {
	Table string
	Where Expr
}

func (s *DeleteStmt) stmtNode() {}
func (s *DeleteStmt) String() string { return "DELETE FROM " + s.Table }

// SelectStmt SELECT
type SelectStmt struct {
	Columns []string // empty = *
	Table   string
	Where   Expr
}

func (s *SelectStmt) stmtNode() {}
func (s *SelectStmt) String() string { return "SELECT FROM " + s.Table }

// --- Index / Security / Maintenance / Transaction ---

// CreateIndexStmt CREATE INDEX
type CreateIndexStmt struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (s *CreateIndexStmt) stmtNode() {}
func (s *CreateIndexStmt) String() string { return "CREATE INDEX " + s.Name }

// DropIndexStmt DROP INDEX
type DropIndexStmt struct {
	Name  string
	Table string
}

func (s *DropIndexStmt) stmtNode() {}
func (s *DropIndexStmt) String() string { return "DROP INDEX " + s.Name }

// CreateUserStmt CREATE USER
type CreateUserStmt struct {
	Username string
	Password string
}

func (s *CreateUserStmt) stmtNode() {}
func (s *CreateUserStmt) String() string { return "CREATE USER " + s.Username }

// GrantStmt GRANT
type GrantStmt struct {
	Privileges []string
	Table      string
	Username   string
}

func (s *GrantStmt) stmtNode() {}
func (s *GrantStmt) String() string { return "GRANT" }

// BackupStmt BACKUP DATABASE
type BackupStmt struct {
	DBName string
	Path   string
}

func (s *BackupStmt) stmtNode() {}
func (s *BackupStmt) String() string { return "BACKUP " + s.DBName }

// RestoreStmt RESTORE DATABASE
type RestoreStmt struct {
	DBName string
	Path   string
}

func (s *RestoreStmt) stmtNode() {}
func (s *RestoreStmt) String() string { return "RESTORE " + s.DBName }

// BeginStmt BEGIN TRANSACTION
type BeginStmt struct{}

func (s *BeginStmt) stmtNode() {}
func (s *BeginStmt) String() string { return "BEGIN TRANSACTION" }

// CommitStmt COMMIT
type CommitStmt struct{}

func (s *CommitStmt) stmtNode() {}
func (s *CommitStmt) String() string { return "COMMIT" }

// RollbackStmt ROLLBACK
type RollbackStmt struct{}

func (s *RollbackStmt) stmtNode() {}
func (s *RollbackStmt) String() string { return "ROLLBACK" }

// --- Expressions ---

// BinaryExpr 二元表达式
type BinaryExpr struct {
	Left  Expr
	Op    string // =, <>, <, >, <=, >=, AND, OR
	Right Expr
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return e.Left.String() + " " + e.Op + " " + e.Right.String()
}

// UnaryExpr 一元表达式
type UnaryExpr struct {
	Op   string // NOT
	Expr Expr
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) String() string { return e.Op + " " + e.Expr.String() }

// LiteralExpr 字面量表达式
type LiteralExpr struct {
	Value types.Value
}

func (e *LiteralExpr) exprNode() {}
func (e *LiteralExpr) String() string { return e.Value.String() }

// ColumnExpr 列引用表达式
type ColumnExpr struct {
	Name string
}

func (e *ColumnExpr) exprNode() {}
func (e *ColumnExpr) String() string { return e.Name }

// InExpr IN表达式
type InExpr struct {
	Column string
	Values []types.Value
}

func (e *InExpr) exprNode() {}
func (e *InExpr) String() string { return e.Column + " IN (...)" }
