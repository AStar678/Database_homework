package executor

import (
	"fmt"

	"github.com/ruanko/dbms/pkg/parser"
)

func execCreateDatabase(ctx *Context, stmt *parser.CreateDatabaseStmt) (*Result, error) {
	if err := ctx.Catalog.CreateDatabase(stmt.Name); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Database '%s' created", stmt.Name)}, nil
}

func execDropDatabase(ctx *Context, stmt *parser.DropDatabaseStmt) (*Result, error) {
	if err := ctx.Catalog.DropDatabase(stmt.Name); err != nil {
		return nil, err
	}
	if ctx.CurrentDB == stmt.Name {
		ctx.CurrentDB = ""
	}
	return &Result{Message: fmt.Sprintf("Database '%s' dropped", stmt.Name)}, nil
}

func execUseDatabase(ctx *Context, stmt *parser.UseDatabaseStmt) (*Result, error) {
	_, err := ctx.Catalog.GetDatabase(stmt.Name)
	if err != nil {
		return nil, err
	}
	ctx.CurrentDB = stmt.Name
	return &Result{Message: fmt.Sprintf("Database changed to '%s'", stmt.Name)}, nil
}

func execShowDatabases(ctx *Context, stmt *parser.ShowDatabasesStmt) (*Result, error) {
	dbs, err := ctx.Catalog.ListDatabases()
	if err != nil {
		return nil, err
	}
	var rows [][]string
	for _, db := range dbs {
		dbType := "USER"
		if !db.Type {
			dbType = "SYSTEM"
		}
		rows = append(rows, []string{db.Name, dbType, db.Filename, db.Crtime.Format("2006-01-02 15:04:05")})
	}
	return &Result{
		Message: fmt.Sprintf("%d database(s) found", len(dbs)),
		Columns: []string{"Name", "Type", "Path", "Created"},
		Rows:    rows,
	}, nil
}
