package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/ruanko/dbms/pkg/executor"
	"github.com/ruanko/dbms/pkg/parser"
)

func main() {
	fmt.Println("Ruanko DBMS [Go Edition]")
	fmt.Println("Type SQL statements ending with ';' or '.exit' to quit.")
	fmt.Println()

	ctx := executor.NewContext()
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("ruanko> ")
		input, err := readStatement(reader)
		if err != nil {
			if input == "" {
				fmt.Println("Bye.")
				return
			}
			// 有内容但遇到EOF，继续处理
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// 处理特殊命令
		lower := strings.ToLower(input)
		if lower == ".exit" || lower == ".quit" || lower == "exit" || lower == "quit" {
			fmt.Println("Bye.")
			return
		}

		// 解析并执行SQL
		stmts, err := parser.Parse(input)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		for _, stmt := range stmts {
			res, err := executor.Execute(ctx, stmt)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResult(res)
		}
	}
}

// readStatement 读取以分号结尾的SQL语句
func readStatement(reader *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return sb.String(), err
		}
		sb.WriteString(line)
		if strings.Contains(line, ";") {
			break
		}
		// 多行输入提示
		fmt.Print("    -> ")
	}
	return sb.String(), nil
}

func printResult(res *executor.Result) {
	if res.Message != "" {
		fmt.Println(res.Message)
	}
	if len(res.Rows) > 0 {
		printTable(res.Columns, res.Rows)
	}
}

func printTable(columns []string, rows [][]string) {
	if len(columns) == 0 {
		return
	}
	// 计算每列宽度
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	// 限制最大宽度
	for i := range widths {
		if widths[i] > 40 {
			widths[i] = 40
		}
	}

	// 打印分隔线
	printSeparator(widths)
	// 打印表头
	for i, col := range columns {
		fmt.Printf("| %-*s ", widths[i], truncate(col, widths[i]))
	}
	fmt.Println("|")
	printSeparator(widths)
	// 打印数据
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				fmt.Printf("| %-*s ", widths[i], truncate(cell, widths[i]))
			}
		}
		fmt.Println("|")
	}
	printSeparator(widths)
	fmt.Printf("%d row(s)\n", len(rows))
}

func printSeparator(widths []int) {
	for _, w := range widths {
		fmt.Printf("+%s", strings.Repeat("-", w+2))
	}
	fmt.Println("+")
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}
