package executor

import (
	"fmt"
	"strings"

	"github.com/ruanko/dbms/pkg/parser"
	"github.com/ruanko/dbms/pkg/storage"
	"github.com/ruanko/dbms/pkg/types"
)

// EvalExpr 在记录上下文中求值表达式
func EvalExpr(expr parser.Expr, record *storage.Record, fieldNames []string) (types.Value, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return e.Value, nil
	case *parser.ColumnExpr:
		for i, name := range fieldNames {
			if strings.EqualFold(name, e.Name) {
				if i < len(record.Values) {
					return record.Values[i], nil
				}
				return &types.NullValue{}, nil
			}
		}
		return &types.NullValue{}, fmt.Errorf("column not found: %s", e.Name)
	case *parser.BinaryExpr:
		return evalBinary(e, record, fieldNames)
	case *parser.UnaryExpr:
		return evalUnary(e, record, fieldNames)
	default:
		return &types.NullValue{}, fmt.Errorf("unknown expression type")
	}
}

func evalBinary(e *parser.BinaryExpr, record *storage.Record, fieldNames []string) (types.Value, error) {
	left, err := EvalExpr(e.Left, record, fieldNames)
	if err != nil {
		return nil, err
	}
	right, err := EvalExpr(e.Right, record, fieldNames)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(e.Op) {
	case "AND":
		lv := isTruthy(left)
		rv := isTruthy(right)
		return &types.BoolValue{V: lv && rv}, nil
	case "OR":
		lv := isTruthy(left)
		rv := isTruthy(right)
		return &types.BoolValue{V: lv || rv}, nil
	case "=":
		return &types.BoolValue{V: types.CompareValues(left, right) == 0}, nil
	case "<>", "!=":
		return &types.BoolValue{V: types.CompareValues(left, right) != 0}, nil
	case "<":
		return &types.BoolValue{V: types.CompareValues(left, right) < 0}, nil
	case ">":
		return &types.BoolValue{V: types.CompareValues(left, right) > 0}, nil
	case "<=":
		return &types.BoolValue{V: types.CompareValues(left, right) <= 0}, nil
	case ">=":
		return &types.BoolValue{V: types.CompareValues(left, right) >= 0}, nil
	default:
		return nil, fmt.Errorf("unknown operator: %s", e.Op)
	}
}

func evalUnary(e *parser.UnaryExpr, record *storage.Record, fieldNames []string) (types.Value, error) {
	val, err := EvalExpr(e.Expr, record, fieldNames)
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(e.Op) {
	case "NOT":
		return &types.BoolValue{V: !isTruthy(val)}, nil
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", e.Op)
	}
}

func isTruthy(v types.Value) bool {
	if types.IsNull(v) {
		return false
	}
	switch tv := v.(type) {
	case *types.BoolValue:
		return tv.V
	case *types.IntValue:
		return tv.V != 0
	case *types.DoubleValue:
		return tv.V != 0
	case *types.VarcharValue:
		return tv.V != ""
	}
	return true
}

// MatchWhere 评估WHERE条件
func MatchWhere(where parser.Expr, record *storage.Record, fieldNames []string) (bool, error) {
	if where == nil {
		return true, nil
	}
	val, err := EvalExpr(where, record, fieldNames)
	if err != nil {
		return false, err
	}
	return isTruthy(val), nil
}
