package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ruanko/dbms/pkg/types"
)

// Parser 语法分析器
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser 创建语法分析器
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens}
}

// Parse 解析SQL，返回语句列表
func Parse(sql string) ([]Stmt, error) {
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, err
	}
	p := NewParser(tokens)
	var stmts []Stmt
	for p.cur().Type != TOKEN_EOF {
		if p.cur().Type == TOKEN_SEMICOLON {
			p.advance()
			continue
		}
		stmt, err := p.parseStmt()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)
		if p.cur().Type == TOKEN_SEMICOLON {
			p.advance()
		}
	}
	return stmts, nil
}

func (p *Parser) cur() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.cur()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) expect(tt TokenType) (Token, error) {
	tok := p.cur()
	if tok.Type != tt {
		return tok, fmt.Errorf("expected %v, got %v (%s)", tt, tok.Type, tok.Literal)
	}
	p.advance()
	return tok, nil
}

func (p *Parser) parseStmt() (Stmt, error) {
	switch p.cur().Type {
	case TOKEN_CREATE:
		return p.parseCreate()
	case TOKEN_DROP:
		return p.parseDrop()
	case TOKEN_ALTER:
		return p.parseAlter()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	case TOKEN_DELETE:
		return p.parseDelete()
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_USE:
		return p.parseUse()
	case TOKEN_SHOW:
		return p.parseShow()
	case TOKEN_GRANT:
		return p.parseGrant()
	case TOKEN_BACKUP:
		return p.parseBackup()
	case TOKEN_RESTORE:
		return p.parseRestore()
	case TOKEN_BEGIN:
		p.advance()
		return &BeginStmt{}, nil
	case TOKEN_COMMIT:
		p.advance()
		return &CommitStmt{}, nil
	case TOKEN_ROLLBACK:
		p.advance()
		return &RollbackStmt{}, nil
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.cur().Literal)
	}
}

// CREATE
func (p *Parser) parseCreate() (Stmt, error) {
	p.advance() // CREATE
	switch p.cur().Type {
	case TOKEN_DATABASE:
		p.advance()
		name := p.advance().Literal
		return &CreateDatabaseStmt{Name: name}, nil
	case TOKEN_TABLE:
		return p.parseCreateTable()
	case TOKEN_INDEX:
		return p.parseCreateIndex()
	case TOKEN_USER:
		return p.parseCreateUser()
	default:
		return nil, fmt.Errorf("expected DATABASE/TABLE/INDEX/USER after CREATE")
	}
}

func (p *Parser) parseCreateTable() (Stmt, error) {
	p.advance() // TABLE
	name := p.advance().Literal
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	var cols []ColumnDef
	for p.cur().Type != TOKEN_RPAREN && p.cur().Type != TOKEN_EOF {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.cur().Type == TOKEN_COMMA {
			p.advance()
		}
	}
	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &CreateTableStmt{Name: name, Columns: cols}, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	name := p.advance().Literal
	typTok := p.advance()
	typ := strings.ToUpper(typTok.Literal)
	param := 0

	// parse type with optional param, e.g. VARCHAR(255)
	if p.cur().Type == TOKEN_LPAREN {
		p.advance()
		n, err := strconv.Atoi(p.advance().Literal)
		if err != nil {
			return ColumnDef{}, err
		}
		param = n
		if _, err := p.expect(TOKEN_RPAREN); err != nil {
			return ColumnDef{}, err
		}
	}

	var constraints []Constraint
	for {
		switch p.cur().Type {
		case TOKEN_NOT:
			p.advance()
			if _, err := p.expect(TOKEN_NULL); err != nil {
				return ColumnDef{}, err
			}
			constraints = append(constraints, Constraint{Type: "NOT_NULL"})
		case TOKEN_PRIMARY:
			p.advance()
			if _, err := p.expect(TOKEN_KEY); err != nil {
				return ColumnDef{}, err
			}
			constraints = append(constraints, Constraint{Type: "PRIMARY_KEY"})
		case TOKEN_UNIQUE:
			p.advance()
			constraints = append(constraints, Constraint{Type: "UNIQUE"})
		case TOKEN_DEFAULT:
			p.advance()
			val := p.advance().Literal
			constraints = append(constraints, Constraint{Type: "DEFAULT", Value: val})
		case TOKEN_IDENTITY:
			p.advance()
			constraints = append(constraints, Constraint{Type: "IDENTITY"})
		case TOKEN_FOREIGN:
			p.advance()
			if _, err := p.expect(TOKEN_KEY); err != nil {
				return ColumnDef{}, err
			}
			// skip REFERENCES table(col)
			if p.cur().Type == TOKEN_IDENTIFIER && strings.ToUpper(p.cur().Literal) == "REFERENCES" {
				p.advance()
				refTable := p.advance().Literal
				constraints = append(constraints, Constraint{Type: "FOREIGN_KEY", Value: refTable})
				if p.cur().Type == TOKEN_LPAREN {
					p.advance()
					p.advance() // col
					p.expect(TOKEN_RPAREN)
				}
			}
		case TOKEN_CHECK:
			p.advance()
			if _, err := p.expect(TOKEN_LPAREN); err != nil {
				return ColumnDef{}, err
			}
			// 简化：只读取直到右括号的内容
			var condParts []string
			for p.cur().Type != TOKEN_RPAREN && p.cur().Type != TOKEN_EOF {
				condParts = append(condParts, p.advance().Literal)
			}
			p.expect(TOKEN_RPAREN)
			constraints = append(constraints, Constraint{Type: "CHECK", Value: strings.Join(condParts, " ")})
		default:
			return ColumnDef{Name: name, Type: typ, Param: param, Constraints: constraints}, nil
		}
	}
}

func (p *Parser) parseCreateIndex() (Stmt, error) {
	p.advance() // INDEX
	name := p.advance().Literal
	if _, err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}
	table := p.advance().Literal
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	var cols []string
	for p.cur().Type != TOKEN_RPAREN && p.cur().Type != TOKEN_EOF {
		cols = append(cols, p.advance().Literal)
		if p.cur().Type == TOKEN_COMMA {
			p.advance()
		}
	}
	p.expect(TOKEN_RPAREN)
	return &CreateIndexStmt{Name: name, Table: table, Columns: cols}, nil
}

func (p *Parser) parseCreateUser() (Stmt, error) {
	p.advance() // USER
	username := p.advance().Literal
	password := ""
	if p.cur().Type == TOKEN_PASSWORD ||
		(p.cur().Type == TOKEN_IDENTIFIER && strings.ToUpper(p.cur().Literal) == "PASSWORD") {
		p.advance()
		password = p.advance().Literal
	}
	return &CreateUserStmt{Username: username, Password: password}, nil
}

// DROP
func (p *Parser) parseDrop() (Stmt, error) {
	p.advance() // DROP
	switch p.cur().Type {
	case TOKEN_DATABASE:
		p.advance()
		return &DropDatabaseStmt{Name: p.advance().Literal}, nil
	case TOKEN_TABLE:
		p.advance()
		return &DropTableStmt{Name: p.advance().Literal}, nil
	case TOKEN_INDEX:
		return p.parseDropIndex()
	case TOKEN_USER:
		p.advance()
		return &DropTableStmt{Name: p.advance().Literal}, nil // reuse for user drop
	default:
		return nil, fmt.Errorf("expected DATABASE/TABLE/INDEX/USER after DROP")
	}
}

func (p *Parser) parseDropIndex() (Stmt, error) {
	p.advance() // INDEX
	name := p.advance().Literal
	table := ""
	if p.cur().Type == TOKEN_ON {
		p.advance()
		table = p.advance().Literal
	}
	return &DropIndexStmt{Name: name, Table: table}, nil
}

// ALTER
func (p *Parser) parseAlter() (Stmt, error) {
	p.advance() // ALTER
	if _, err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}
	tableName := p.advance().Literal
	switch p.cur().Type {
	case TOKEN_ADD:
		p.advance()
		if p.cur().Type == TOKEN_COLUMN {
			p.advance()
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		return &AlterTableStmt{Name: tableName, Action: AlterAddColumn, ColumnDef: &col}, nil
	case TOKEN_MODIFY:
		p.advance()
		if p.cur().Type == TOKEN_COLUMN {
			p.advance()
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		return &AlterTableStmt{Name: tableName, Action: AlterModifyColumn, ColumnDef: &col}, nil
	case TOKEN_DROP:
		p.advance()
		if p.cur().Type == TOKEN_COLUMN {
			p.advance()
		}
		colName := p.advance().Literal
		return &AlterTableStmt{Name: tableName, Action: AlterDropColumn, ColumnName: colName}, nil
	default:
		return nil, fmt.Errorf("expected ADD/MODIFY/DROP after ALTER TABLE")
	}
}

// INSERT
func (p *Parser) parseInsert() (Stmt, error) {
	p.advance() // INSERT
	if _, err := p.expect(TOKEN_INTO); err != nil {
		return nil, err
	}
	table := p.advance().Literal
	var cols []string
	if p.cur().Type == TOKEN_LPAREN {
		p.advance()
		for p.cur().Type != TOKEN_RPAREN && p.cur().Type != TOKEN_EOF {
			cols = append(cols, p.advance().Literal)
			if p.cur().Type == TOKEN_COMMA {
				p.advance()
			}
		}
		p.expect(TOKEN_RPAREN)
	}
	if _, err := p.expect(TOKEN_VALUES); err != nil {
		return nil, err
	}
	var rows [][]types.Value
	for {
		if _, err := p.expect(TOKEN_LPAREN); err != nil {
			return nil, err
		}
		var vals []types.Value
		for p.cur().Type != TOKEN_RPAREN && p.cur().Type != TOKEN_EOF {
			val, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
			if p.cur().Type == TOKEN_COMMA {
				p.advance()
			}
		}
		p.expect(TOKEN_RPAREN)
		rows = append(rows, vals)
		if p.cur().Type == TOKEN_COMMA {
			p.advance()
		} else {
			break
		}
	}
	return &InsertStmt{Table: table, Columns: cols, Values: rows}, nil
}

func (p *Parser) parseValue() (types.Value, error) {
	tok := p.advance()
	switch tok.Type {
	case TOKEN_NUMBER:
		v, err := strconv.ParseInt(tok.Literal, 10, 32)
		if err != nil {
			return nil, err
		}
		return &types.IntValue{V: int32(v)}, nil
	case TOKEN_FLOAT:
		v, err := strconv.ParseFloat(tok.Literal, 64)
		if err != nil {
			return nil, err
		}
		return &types.DoubleValue{V: v}, nil
	case TOKEN_STRING:
		return &types.VarcharValue{V: tok.Literal}, nil
	case TOKEN_TRUE:
		return &types.BoolValue{V: true}, nil
	case TOKEN_FALSE:
		return &types.BoolValue{V: false}, nil
	case TOKEN_NULL:
		return &types.NullValue{}, nil
	default:
		return nil, fmt.Errorf("unexpected value token: %s", tok.Literal)
	}
}

// UPDATE
func (p *Parser) parseUpdate() (Stmt, error) {
	p.advance() // UPDATE
	table := p.advance().Literal
	if _, err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}
	var sets []SetClause
	for {
		col := p.advance().Literal
		if _, err := p.expect(TOKEN_EQ); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sets = append(sets, SetClause{Column: col, Value: val})
		if p.cur().Type == TOKEN_COMMA {
			p.advance()
		} else {
			break
		}
	}
	var where Expr
	if p.cur().Type == TOKEN_WHERE {
		p.advance()
		where, _ = p.parseExpr()
	}
	return &UpdateStmt{Table: table, Sets: sets, Where: where}, nil
}

// DELETE
func (p *Parser) parseDelete() (Stmt, error) {
	p.advance() // DELETE
	if p.cur().Type == TOKEN_FROM {
		p.advance()
	}
	table := p.advance().Literal
	var where Expr
	if p.cur().Type == TOKEN_WHERE {
		p.advance()
		where, _ = p.parseExpr()
	}
	return &DeleteStmt{Table: table, Where: where}, nil
}

// SELECT
func (p *Parser) parseSelect() (Stmt, error) {
	p.advance() // SELECT
	var cols []string
	if p.cur().Type == TOKEN_STAR {
		p.advance()
	} else {
		for {
			cols = append(cols, p.advance().Literal)
			if p.cur().Type == TOKEN_COMMA {
				p.advance()
			} else {
				break
			}
		}
	}
	if _, err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	table := p.advance().Literal
	var where Expr
	if p.cur().Type == TOKEN_WHERE {
		p.advance()
		where, _ = p.parseExpr()
	}
	return &SelectStmt{Columns: cols, Table: table, Where: where}, nil
}

// USE
func (p *Parser) parseUse() (Stmt, error) {
	p.advance()
	return &UseDatabaseStmt{Name: p.advance().Literal}, nil
}

// SHOW
func (p *Parser) parseShow() (Stmt, error) {
	p.advance()
	switch p.cur().Type {
	case TOKEN_DATABASES:
		p.advance()
		return &ShowDatabasesStmt{}, nil
	case TOKEN_TABLES:
		p.advance()
		return &ShowTablesStmt{}, nil
	default:
		return nil, fmt.Errorf("expected DATABASES or TABLES after SHOW")
	}
}

// GRANT
func (p *Parser) parseGrant() (Stmt, error) {
	p.advance() // GRANT
	var privs []string
	for {
		privs = append(privs, p.advance().Literal)
		if p.cur().Type == TOKEN_COMMA {
			p.advance()
		} else {
			break
		}
	}
	if _, err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}
	table := p.advance().Literal
	if _, err := p.expect(TOKEN_TO); err != nil {
		return nil, err
	}
	user := p.advance().Literal
	return &GrantStmt{Privileges: privs, Table: table, Username: user}, nil
}

// BACKUP / RESTORE
func (p *Parser) parseBackup() (Stmt, error) {
	p.advance() // BACKUP
	if p.cur().Type == TOKEN_DATABASE {
		p.advance()
	}
	dbName := p.advance().Literal
	if _, err := p.expect(TOKEN_TO); err != nil {
		return nil, err
	}
	path := p.advance().Literal
	return &BackupStmt{DBName: dbName, Path: path}, nil
}

func (p *Parser) parseRestore() (Stmt, error) {
	p.advance() // RESTORE
	if p.cur().Type == TOKEN_DATABASE {
		p.advance()
	}
	dbName := p.advance().Literal
	if p.cur().Type != TOKEN_FROM && p.cur().Type != TOKEN_TO {
		return nil, fmt.Errorf("expected FROM or TO after database name in RESTORE, got %s", p.cur().Literal)
	}
	p.advance() // FROM or TO
	path := p.advance().Literal
	return &RestoreStmt{DBName: dbName, Path: path}, nil
}

// --- Expression Parsing ---

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur().Type == TOKEN_OR {
		op := p.advance().Literal
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: strings.ToUpper(op), Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur().Type == TOKEN_AND {
		op := p.advance().Literal
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: strings.ToUpper(op), Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.cur().Type == TOKEN_NOT {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	switch p.cur().Type {
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE:
		op := p.advance().Literal
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}
	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	tok := p.cur()
	switch tok.Type {
	case TOKEN_NUMBER:
		p.advance()
		v, _ := strconv.ParseInt(tok.Literal, 10, 32)
		return &LiteralExpr{Value: &types.IntValue{V: int32(v)}}, nil
	case TOKEN_FLOAT:
		p.advance()
		v, _ := strconv.ParseFloat(tok.Literal, 64)
		return &LiteralExpr{Value: &types.DoubleValue{V: v}}, nil
	case TOKEN_STRING:
		p.advance()
		return &LiteralExpr{Value: &types.VarcharValue{V: tok.Literal}}, nil
	case TOKEN_TRUE:
		p.advance()
		return &LiteralExpr{Value: &types.BoolValue{V: true}}, nil
	case TOKEN_FALSE:
		p.advance()
		return &LiteralExpr{Value: &types.BoolValue{V: false}}, nil
	case TOKEN_NULL:
		p.advance()
		return &LiteralExpr{Value: &types.NullValue{}}, nil
	case TOKEN_IDENTIFIER:
		p.advance()
		return &ColumnExpr{Name: tok.Literal}, nil
	case TOKEN_LPAREN:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.expect(TOKEN_RPAREN)
		return expr, nil
	default:
		return nil, fmt.Errorf("unexpected expression token: %s", tok.Literal)
	}
}
