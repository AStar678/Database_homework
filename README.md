# Ruanko-DBMS-Go

Ruanko DBMS 是一个基于文件存储的关系型数据库管理系统，使用 Go 语言实现。它采用了四层架构（解析层 → 执行层 → 元数据 / 存储层 → 物理文件层），支持完整的 SQL 解析、DDL/DML/DQL 执行引擎以及二进制文件存储引擎，并预留了 GUI 扩展接口。

## 功能特性

### 核心功能（A/B 级需求）

- **数据库管理**：创建、删除、切换数据库
- **表管理**：创建、修改、删除表
- **字段管理**：添加、修改、删除字段
- **数据操作**：插入、更新、查询、删除记录
- **SQL 解析器**：手写递归下降解析器，支持标准 SQL 语法
- **表达式求值**：支持 WHERE 条件过滤（AND/OR/NOT、比较运算）

### 扩展功能（C 级需求简化实现）

- **索引管理**：内存索引映射框架（预留 .ix 接口）
- **事务管理**：支持 BEGIN / COMMIT / ROLLBACK 命令
- **完整性约束**：NOT NULL、PRIMARY KEY、UNIQUE 检查
- **安全性管理**：CREATE USER、GRANT（简化版）
- **数据库维护**：BACKUP DATABASE / RESTORE DATABASE（简化版）

## 项目结构

```
ruanko/
├── cmd/ruanko/main.go      # 交互式 CLI 入口
├── pkg/
│   ├── common/             # 常量、路径管理、序列化工具
│   ├── types/              # 数据类型系统（INTEGER/BOOL/DOUBLE/VARCHAR/DATETIME）
│   ├── storage/            # 二进制文件存储引擎
│   ├── catalog/            # 元数据管理（数据库/表/字段）
│   ├── parser/             # SQL 词法分析器 + 语法分析器 + AST
│   ├── executor/           # 执行引擎（DDL/DML/DQL + 表达式求值）
│   ├── integrity/          # 完整性约束检查器
│   ├── index/              # 索引管理器（简化框架）
│   ├── transaction/        # 事务管理（简化框架）
│   └── security/           # 安全性管理（简化框架）
└── README.md
```

## 编译与运行

### 环境要求

- Go 1.22 或更高版本
- macOS / Linux / Windows

### 编译

```bash
cd ruanko
go build -o ruanko_cli cmd/ruanko/main.go
```

### 运行

```bash
./ruanko_cli
```

启动后将进入交互式命令行：

```
Ruanko DBMS [Go Edition]
Type SQL statements ending with ';' or '.exit' to quit.

ruanko> 
```

输入 `.exit` 或 `.quit` 退出程序。

## 支持的数据类型

| 类型      | 大小（对齐后） | 说明                          |
|-----------|---------------|-------------------------------|
| INTEGER   | 4 bytes       | 32 位有符号整数               |
| BOOL      | 4 bytes       | 布尔类型（true/false）        |
| DOUBLE    | 8 bytes       | 64 位浮点数                   |
| VARCHAR   | (n+1) 对齐到4  | 变长字符串，最长 255 字符     |
| DATETIME  | 16 bytes      | 日期时间类型                  |

## 支持的 SQL 语句

### DDL - 数据定义语言

```sql
-- 数据库操作
CREATE DATABASE dbname;
DROP DATABASE dbname;
USE dbname;
SHOW DATABASES;

-- 表操作
CREATE TABLE tablename (
    col1 INTEGER PRIMARY KEY,
    col2 VARCHAR(50) NOT NULL,
    col3 DOUBLE UNIQUE,
    col4 DATETIME DEFAULT '2024-01-01'
);
DROP TABLE tablename;
SHOW TABLES;

-- 字段操作
ALTER TABLE tablename ADD COLUMN colname INTEGER;
ALTER TABLE tablename MODIFY COLUMN colname VARCHAR(100);
ALTER TABLE tablename DROP COLUMN colname;
```

### DML - 数据操作语言

```sql
-- 插入记录
INSERT INTO tablename (col1, col2, col3) VALUES (1, 'hello', 3.14);
INSERT INTO tablename VALUES (1, 'hello', 3.14), (2, 'world', 2.71);

-- 更新记录
UPDATE tablename SET col2 = 'updated', col3 = 9.8 WHERE col1 = 1;

-- 删除记录
DELETE FROM tablename WHERE col1 = 1;
```

### DQL - 数据查询语言

```sql
-- 查询所有列
SELECT * FROM tablename;

-- 查询指定列
SELECT col1, col2 FROM tablename;

-- 带条件查询
SELECT * FROM tablename WHERE col1 = 1 AND col2 > 10;
SELECT * FROM tablename WHERE col1 <> 2 OR col3 <= 100;
```

### 事务控制

```sql
BEGIN;
COMMIT;
ROLLBACK;
```

### 其他命令

```sql
CREATE INDEX idx_name ON tablename (col1);
DROP INDEX idx_name;

CREATE USER username PASSWORD 'password';
GRANT SELECT, INSERT ON tablename TO username;

BACKUP DATABASE dbname TO '/backup/path';
RESTORE DATABASE dbname FROM '/backup/path';
```

## 完整使用示例

以下是一个从创建数据库到删除数据库的完整操作流程：

```sql
-- 1. 创建数据库
CREATE DATABASE school;

-- 2. 切换到该数据库
USE school;

-- 3. 创建学生表
CREATE TABLE students (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INTEGER,
    score DOUBLE,
    enroll_date DATETIME
);

-- 4. 插入数据
INSERT INTO students (id, name, age, score, enroll_date)
VALUES (1, '张三', 20, 85.5, '2023-09-01');

INSERT INTO students VALUES
(2, '李四', 21, 92.0, '2023-09-01'),
(3, '王五', 19, 78.5, '2023-09-02');

-- 5. 查询所有学生
SELECT * FROM students;

-- 6. 查询成绩大于 80 分的学生
SELECT name, score FROM students WHERE score > 80;

-- 7. 查询特定学生
SELECT * FROM students WHERE id = 2;

-- 8. 更新学生成绩
UPDATE students SET score = 88.0 WHERE id = 1;

-- 9. 删除学生
DELETE FROM students WHERE id = 3;

-- 10. 验证删除结果
SELECT * FROM students;

-- 11. 添加新字段
ALTER TABLE students ADD COLUMN gender VARCHAR(10);

-- 12. 查看所有表
SHOW TABLES;

-- 13. 删除表
DROP TABLE students;

-- 14. 删除数据库
DROP DATABASE school;
```

### 预期输出

```
Database 'school' created
Database changed to 'school'
Table 'students' created
1 row(s) inserted
2 row(s) inserted
3 row(s) returned
+----+--------+-----+-------+---------------------+
| id | name   | age | score | enroll_date         |
+----+--------+-----+-------+---------------------+
| 1  | 张三   | 20  | 85.5  | 2023-09-01 00:00:00 |
| 2  | 李四   | 21  | 92    | 2023-09-01 00:00:00 |
| 3  | 王五   | 19  | 78.5  | 2023-09-02 00:00:00 |
+----+--------+-----+-------+---------------------+
3 row(s)

2 row(s) returned
+--------+-------+
| name   | score |
+--------+-------+
| 张三   | 85.5  |
| 李四   | 92    |
+--------+-------+
2 row(s)

1 row(s) updated
1 row(s) deleted
2 row(s) returned
+----+--------+-----+-------+---------------------+
| id | name   | age | score | enroll_date         |
+----+--------+-----+-------+---------------------+
| 1  | 张三   | 20  | 88    | 2023-09-01 00:00:00 |
| 2  | 李四   | 21  | 92    | 2023-09-01 00:00:00 |
+----+--------+-----+-------+---------------------+
2 row(s)

Column 'gender' added to table 'students'
1 table(s) found
Table 'students' dropped
Database 'school' dropped
```

## 数据文件格式

所有数据以二进制文件形式存储在工作目录下的 `dbms_root/` 文件夹中：

```
dbms_root/
├── ruanko.db               # 数据库描述文件（系统目录）
└── data/
    ├── Ruanko/             # 系统数据库
    │   ├── Ruanko.tb       # 表描述文件
    │   └── Ruanko.log      # 日志文件
    └── [dbname]/           # 用户数据库
        ├── [dbname].tb     # 表描述文件
        ├── [dbname].log    # 日志文件
        ├── [table].tdf     # 表定义文件（字段信息）
        ├── [table].trd     # 记录文件（数据）
        ├── [table].tic     # 完整性描述文件
        ├── [table].tid     # 索引描述文件
        └── [index].ix      # 索引数据文件
```

可以通过设置环境变量 `DBMS_ROOT` 来指定数据存储路径：

```bash
export DBMS_ROOT=/path/to/dbms_root
./ruanko_cli
```

## 注意事项

1. **系统数据库保护**：名为 `Ruanko` 的系统数据库不允许删除。
2. **名称长度限制**：数据库名、表名、字段名长度均不得超过 128 个字符。
3. **VARCHAR 长度**：VARCHAR 类型参数范围为 1-255。
4. **删除策略**：记录删除采用标记删除策略，被删除的记录在文件中保留但查询时不可见。
5. **C 级功能**：索引、事务、安全性、备份恢复等功能为简化框架实现，主要用于演示和扩展预留。
6. **GUI 扩展**：所有核心逻辑封装在 `pkg/` 包中，CLI 仅为薄层调用，可方便地为图形界面复用。

## 技术栈

- **语言**：Go 1.22+
- **架构**：分层架构（Parser -> Executor -> Catalog/Storage -> Binary Files）
- **SQL 解析**：手写递归下降解析器
- **存储引擎**：基于二进制文件的块存储引擎
- **数据对齐**：严格遵循 4 字节对齐规则
