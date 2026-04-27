package executor

import (
	"os"
	"testing"

	"github.com/ruanko/dbms/pkg/common"
	"github.com/ruanko/dbms/pkg/parser"
)

func TestBasicFlow(t *testing.T) {
	// 清理测试环境
	os.RemoveAll(common.DBMSRoot())
	ctx := NewContext()

	// 1. Create database
	stmts, err := parser.Parse("CREATE DATABASE testdb;")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	res, err := Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("create db error: %v", err)
	}
	t.Logf("Create DB: %s", res.Message)

	// 2. Use database
	stmts, _ = parser.Parse("USE testdb;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("use db error: %v", err)
	}
	t.Logf("Use DB: %s", res.Message)

	// 3. Create table
	stmts, _ = parser.Parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL, age INTEGER);")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	t.Logf("Create Table: %s", res.Message)

	// 4. Insert records
	stmts, _ = parser.Parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30);")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("insert error: %v", err)
	}
	t.Logf("Insert: %s", res.Message)

	// 5. Select all
	stmts, _ = parser.Parse("SELECT * FROM users;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("select error: %v", err)
	}
	t.Logf("Select: %s", res.Message)
	for _, row := range res.Rows {
		t.Logf("  Row: %v", row)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	// 6. Update
	stmts, _ = parser.Parse("UPDATE users SET age = 26 WHERE id = 1;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("update error: %v", err)
	}
	t.Logf("Update: %s", res.Message)

	// 7. Select with WHERE
	stmts, _ = parser.Parse("SELECT name, age FROM users WHERE id = 1;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("select where error: %v", err)
	}
	t.Logf("Select WHERE: %s", res.Message)
	if len(res.Rows) != 1 || res.Rows[0][1] != "26" {
		t.Fatalf("expected age=26, got %v", res.Rows)
	}

	// 8. Delete
	stmts, _ = parser.Parse("DELETE FROM users WHERE id = 2;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("delete error: %v", err)
	}
	t.Logf("Delete: %s", res.Message)

	// 9. Verify delete
	stmts, _ = parser.Parse("SELECT * FROM users;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("select after delete error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(res.Rows))
	}

	// 10. Drop table
	stmts, _ = parser.Parse("DROP TABLE users;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("drop table error: %v", err)
	}
	t.Logf("Drop Table: %s", res.Message)

	// 11. Drop database
	stmts, _ = parser.Parse("DROP DATABASE testdb;")
	res, err = Execute(ctx, stmts[0])
	if err != nil {
		t.Fatalf("drop db error: %v", err)
	}
	t.Logf("Drop DB: %s", res.Message)
}
