-- ============================================================
-- Ruanko DBMS 终端功能演示脚本
-- ============================================================

-- ========== 1. 系统与数据库管理 ==========
SHOW DATABASES;
CREATE DATABASE shop;
SHOW DATABASES;
USE shop;

-- ========== 2. 表创建与字段定义（覆盖所有数据类型） ==========
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DOUBLE,
    stock INTEGER DEFAULT 0,
    created_at DATETIME,
    is_active BOOL
);
SHOW TABLES;

-- ========== 3. 数据插入（多种数据类型） ==========
INSERT INTO products VALUES
(1, 'iPhone 15', 5999.00, 100, '2024-01-15 10:30:00', true),
(2, 'MacBook Pro', 14999.00, 50, '2024-02-20 14:00:00', true),
(3, 'AirPods Pro', 1999.00, 200, '2024-03-10 09:00:00', true),
(4, 'iPad Mini', 3999.00, 0, '2024-01-20 11:00:00', false),
(5, 'Magic Mouse', 699.00, 500, '2024-05-01 16:45:00', true);

-- ========== 4. 全表查询 ==========
SELECT * FROM products;

-- ========== 5. 条件查询（比较运算） ==========
SELECT name, price FROM products WHERE price > 5000;
SELECT * FROM products WHERE stock = 0;
SELECT * FROM products WHERE price <= 1000;

-- ========== 6. 条件查询（逻辑运算 AND / OR） ==========
SELECT name, price, stock FROM products WHERE stock > 0 AND price < 10000;
SELECT * FROM products WHERE stock = 0 OR is_active = false;

-- ========== 7. 更新记录 ==========
UPDATE products SET stock = 150 WHERE id = 1;
UPDATE products SET price = 13999.00, stock = 80 WHERE id = 2;
SELECT * FROM products WHERE id IN (1, 2);

-- ========== 8. 删除记录 ==========
DELETE FROM products WHERE id = 4;
SELECT * FROM products;

-- ========== 9. 表结构修改（添加/删除字段） ==========
ALTER TABLE products ADD COLUMN category VARCHAR(50);
ALTER TABLE products DROP COLUMN category;

-- ========== 10. 索引管理（框架演示） ==========
CREATE INDEX idx_price ON products (price);
CREATE INDEX idx_name ON products (name);
DROP INDEX idx_price;

-- ========== 11. 事务控制（简化版） ==========
BEGIN;
INSERT INTO products VALUES (6, 'Apple Watch', 2999.00, 300, '2024-04-01 08:00:00', true);
SELECT * FROM products WHERE id = 6;
ROLLBACK;
SELECT * FROM products WHERE id = 6;

-- ========== 12. 用户与权限（简化版） ==========
CREATE USER admin PASSWORD 'admin123';
GRANT SELECT, INSERT ON products TO admin;

-- ========== 13. 数据库维护（简化版） ==========
BACKUP DATABASE shop TO './backup';
RESTORE DATABASE shop FROM './backup';

-- ========== 14. 清理与删除 ==========
DROP TABLE products;
SHOW TABLES;
DROP DATABASE shop;
SHOW DATABASES;
.exit
