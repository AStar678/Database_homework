package transaction

// Transaction 事务（简化框架）
type Transaction struct {
	Active bool
	Log    []string // 操作日志
}

// NewTransaction 创建事务
func NewTransaction() *Transaction {
	return &Transaction{}
}

// Begin 开始事务
func (tx *Transaction) Begin() {
	tx.Active = true
	tx.Log = []string{}
}

// Commit 提交事务
func (tx *Transaction) Commit() {
	tx.Active = false
	tx.Log = []string{}
}

// Rollback 回滚事务（简化版：仅记录日志，实际回滚需配合存储层）
func (tx *Transaction) Rollback() {
	tx.Active = false
	tx.Log = []string{}
}

// LogOperation 记录操作
func (tx *Transaction) LogOperation(op string) {
	if tx.Active {
		tx.Log = append(tx.Log, op)
	}
}
