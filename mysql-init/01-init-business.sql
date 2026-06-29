-- ============================================================
-- Smart Scenic BigData Platform - Business Data Init
-- Auto-runs on first MySQL container start (/docker-entrypoint-initdb.d)
-- ============================================================

USE scenic;

-- ============================================================
-- 1. Scenic table t_scenic
-- ============================================================
CREATE TABLE IF NOT EXISTS t_scenic (
    scenic_id     VARCHAR(20) PRIMARY KEY COMMENT 'Scenic ID',
    scenic_name   VARCHAR(100) NOT NULL COMMENT 'Scenic name',
    scenic_type   VARCHAR(50) COMMENT 'Type: natural/culture/entertainment',
    location      VARCHAR(200) COMMENT 'Location',
    open_time     VARCHAR(50) COMMENT 'Open hours',
    description   TEXT COMMENT 'Description',
    ticket_price  DECIMAL(10,2) DEFAULT 0.00 COMMENT 'Ticket price',
    create_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- 2. Visitor table t_visitor
-- ============================================================
CREATE TABLE IF NOT EXISTS t_visitor (
    visitor_id     VARCHAR(20) PRIMARY KEY COMMENT 'Visitor ID',
    visitor_name   VARCHAR(50) COMMENT 'Name',
    gender         CHAR(2) COMMENT 'Gender',
    age            INT COMMENT 'Age',
    age_group      VARCHAR(20) COMMENT 'Age group',
    phone          VARCHAR(20) COMMENT 'Phone',
    region         VARCHAR(50) COMMENT 'From region',
    register_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- 3. Consume table t_consume
-- ============================================================
CREATE TABLE IF NOT EXISTS t_consume (
    consume_id     INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Consume ID',
    visitor_id     VARCHAR(20) NOT NULL COMMENT 'Visitor ID',
    scenic_id      VARCHAR(20) NOT NULL COMMENT 'Scenic ID',
    consume_type   VARCHAR(50) COMMENT 'Type: ticket/food/hotel/shop',
    amount         DECIMAL(10,2) DEFAULT 0.00 COMMENT 'Amount',
    consume_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- 4. Visit table t_visit
-- ============================================================
CREATE TABLE IF NOT EXISTS t_visit (
    visit_id       INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Visit ID',
    visitor_id     VARCHAR(20) NOT NULL COMMENT 'Visitor ID',
    scenic_id      VARCHAR(20) NOT NULL COMMENT 'Scenic ID',
    entry_time     DATETIME COMMENT 'Entry time',
    exit_time      DATETIME COMMENT 'Exit time',
    duration_min   INT COMMENT 'Duration minutes',
    satisfaction   INT DEFAULT 5 COMMENT 'Rating 1-5'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- 5. Review table t_review (extension)
-- ============================================================
CREATE TABLE IF NOT EXISTS t_review (
    review_id      INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Review ID',
    visitor_id     VARCHAR(20) NOT NULL COMMENT 'Visitor ID',
    scenic_id      VARCHAR(20) NOT NULL COMMENT 'Scenic ID',
    rating         INT DEFAULT 5 COMMENT 'Rating 1-5',
    comment        VARCHAR(500) COMMENT 'Comment text',
    review_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- Seed data: t_scenic (10 rows)
-- ============================================================
REPLACE INTO t_scenic (scenic_id, scenic_name, scenic_type, location, open_time, description, ticket_price) VALUES
('S001', 'West Lake', 'natural', 'Hangzhou Zhejiang', 'all day', 'World heritage, famous lake', 0.00),
('S002', 'Forbidden City', 'culture', 'Beijing', '08:30-17:00', 'Ming Qing royal palace', 60.00),
('S003', 'Zhangjiajie', 'natural', 'Zhangjiajie Hunan', '07:00-18:00', 'World natural heritage', 225.00),
('S004', 'Disneyland', 'entertainment', 'Shanghai', '09:00-21:00', 'Theme park', 475.00),
('S005', 'Huangshan', 'natural', 'Huangshan Anhui', '06:00-17:30', 'Five mountains return no look', 190.00),
('S006', 'Terracotta Army', 'culture', 'Xian Shaanxi', '08:30-17:00', 'Qin dynasty mausoleum', 120.00),
('S007', 'Jiuzhaigou', 'natural', 'Aba Sichuan', '07:00-18:00', 'Fairy tale world', 220.00),
('S008', 'The Bund', 'culture', 'Shanghai', 'all day', 'Modern skyline', 0.00),
('S009', 'Gulangyu', 'culture', 'Xiamen Fujian', 'all day', 'Piano island', 100.00),
('S010', 'Mount Tai', 'natural', 'Tai An Shandong', 'all day', 'Five great mountains', 115.00);

-- ============================================================
-- Seed data: t_visitor (20 rows)
-- ============================================================
REPLACE INTO t_visitor (visitor_id, visitor_name, gender, age, age_group, phone, region) VALUES
('V001', 'Alice', 'F', 25, 'youth', '13800000001', 'Beijing'),
('V002', 'Bob', 'M', 35, 'youth', '13800000002', 'Shanghai'),
('V003', 'Carol', 'F', 45, 'middle', '13800000003', 'Guangdong'),
('V004', 'David', 'M', 28, 'youth', '13800000004', 'Beijing'),
('V005', 'Eve', 'F', 32, 'youth', '13800000005', 'Zhejiang'),
('V006', 'Frank', 'M', 55, 'senior', '13800000006', 'Hubei'),
('V007', 'Grace', 'F', 22, 'youth', '13800000007', 'Sichuan'),
('V008', 'Henry', 'M', 40, 'middle', '13800000008', 'Shanghai'),
('V009', 'Ivy', 'F', 30, 'youth', '13800000009', 'Fujian'),
('V010', 'Jack', 'M', 60, 'senior', '13800000010', 'Shandong'),
('V011', 'Kate', 'F', 26, 'youth', '13800000011', 'Hunan'),
('V012', 'Leo', 'M', 38, 'middle', '13800000012', 'Shaanxi'),
('V013', 'Mia', 'F', 50, 'senior', '13800000013', 'Yunnan'),
('V014', 'Nick', 'M', 24, 'youth', '13800000014', 'Jiangsu'),
('V015', 'Olive', 'F', 33, 'youth', '13800000015', 'Hebei'),
('V016', 'Paul', 'M', 42, 'middle', '13800000016', 'Liaoning'),
('V017', 'Queen', 'F', 29, 'youth', '13800000017', 'Guangxi'),
('V018', 'Roy', 'M', 47, 'middle', '13800000018', 'Jilin'),
('V019', 'Sara', 'F', 36, 'middle', '13800000019', 'Heilongjiang'),
('V020', 'Tom', 'M', 65, 'senior', '13800000020', 'Guangdong');

-- ============================================================
-- Seed data: t_consume (32 rows)
-- ============================================================
REPLACE INTO t_consume (consume_id, visitor_id, scenic_id, consume_type, amount) VALUES
(1, 'V001', 'S001', 'ticket', 0.00),
(2, 'V001', 'S001', 'food', 80.00),
(3, 'V001', 'S001', 'shop', 50.00),
(4, 'V002', 'S002', 'ticket', 60.00),
(5, 'V002', 'S002', 'food', 120.00),
(6, 'V003', 'S003', 'ticket', 225.00),
(7, 'V003', 'S003', 'hotel', 380.00),
(8, 'V004', 'S004', 'ticket', 475.00),
(9, 'V004', 'S004', 'food', 150.00),
(10, 'V004', 'S004', 'shop', 200.00),
(11, 'V005', 'S005', 'ticket', 190.00),
(12, 'V005', 'S005', 'hotel', 280.00),
(13, 'V006', 'S006', 'ticket', 120.00),
(14, 'V006', 'S006', 'food', 60.00),
(15, 'V007', 'S007', 'ticket', 220.00),
(16, 'V007', 'S007', 'hotel', 450.00),
(17, 'V008', 'S008', 'food', 100.00),
(18, 'V009', 'S009', 'ticket', 100.00),
(19, 'V009', 'S009', 'food', 80.00),
(20, 'V010', 'S010', 'ticket', 115.00),
(21, 'V010', 'S010', 'hotel', 200.00),
(22, 'V011', 'S001', 'ticket', 0.00),
(23, 'V012', 'S002', 'ticket', 60.00),
(24, 'V013', 'S007', 'ticket', 220.00),
(25, 'V014', 'S004', 'ticket', 475.00),
(26, 'V015', 'S010', 'ticket', 115.00),
(27, 'V016', 'S002', 'food', 150.00),
(28, 'V017', 'S004', 'food', 200.00),
(29, 'V018', 'S007', 'shop', 350.00),
(30, 'V019', 'S001', 'food', 80.00),
(31, 'V020', 'S008', 'food', 120.00),
(32, 'V020', 'S008', 'shop', 80.00);

-- ============================================================
-- Seed data: t_visit (20 rows)
-- ============================================================
REPLACE INTO t_visit (visit_id, visitor_id, scenic_id, entry_time, exit_time, duration_min, satisfaction) VALUES
(1, 'V001', 'S001', '2024-10-01 09:00:00', '2024-10-01 12:30:00', 210, 5),
(2, 'V002', 'S002', '2024-10-02 08:30:00', '2024-10-02 15:00:00', 390, 5),
(3, 'V003', 'S003', '2024-10-03 07:00:00', '2024-10-03 18:00:00', 660, 5),
(4, 'V004', 'S004', '2024-10-04 09:00:00', '2024-10-04 21:00:00', 720, 5),
(5, 'V005', 'S005', '2024-10-05 06:00:00', '2024-10-05 17:30:00', 690, 5),
(6, 'V006', 'S006', '2024-10-06 08:30:00', '2024-10-06 14:00:00', 330, 4),
(7, 'V007', 'S007', '2024-10-07 07:00:00', '2024-10-07 19:00:00', 720, 5),
(8, 'V008', 'S008', '2024-10-08 19:00:00', '2024-10-08 22:00:00', 180, 4),
(9, 'V009', 'S009', '2024-10-09 10:00:00', '2024-10-09 17:00:00', 420, 5),
(10, 'V010', 'S010', '2024-10-10 22:00:00', '2024-10-11 06:00:00', 480, 5),
(11, 'V011', 'S001', '2024-10-11 09:30:00', '2024-10-11 12:00:00', 150, 5),
(12, 'V012', 'S002', '2024-10-12 08:30:00', '2024-10-12 13:00:00', 270, 4),
(13, 'V013', 'S007', '2024-10-13 07:00:00', '2024-10-13 17:00:00', 600, 5),
(14, 'V014', 'S004', '2024-10-14 09:00:00', '2024-10-14 19:00:00', 600, 4),
(15, 'V015', 'S010', '2024-10-15 17:30:00', '2024-10-15 20:30:00', 180, 4),
(16, 'V016', 'S002', '2024-10-16 10:00:00', '2024-10-16 13:00:00', 180, 3),
(17, 'V017', 'S004', '2024-10-17 09:00:00', '2024-10-17 18:00:00', 540, 5),
(18, 'V018', 'S007', '2024-10-18 07:30:00', '2024-10-18 16:30:00', 540, 5),
(19, 'V019', 'S001', '2024-10-19 09:00:00', '2024-10-19 12:30:00', 210, 4),
(20, 'V020', 'S008', '2024-10-20 18:30:00', '2024-10-20 22:00:00', 210, 5);

-- ============================================================
-- Seed data: t_review (10 rows, extension table)
-- ============================================================
REPLACE INTO t_review (review_id, visitor_id, scenic_id, rating, comment) VALUES
(1, 'V001', 'S001', 5, 'West Lake is beautiful'),
(2, 'V002', 'S002', 5, 'Forbidden City is huge'),
(3, 'V003', 'S003', 4, 'Zhangjiajie scenery is amazing'),
(4, 'V004', 'S004', 4, 'Disneyland kids loved it'),
(5, 'V005', 'S005', 5, 'Huangshan sunrise is stunning'),
(6, 'V006', 'S006', 3, 'Terracotta Army is worth seeing'),
(7, 'V007', 'S007', 5, 'Jiuzhaigou water is crystal clear'),
(8, 'V008', 'S008', 4, 'The Bund night view is wonderful'),
(9, 'V009', 'S009', 5, 'Gulangyu is artsy'),
(10, 'V010', 'S010', 4, 'Mount Tai sunrise worth early wake');

-- ============================================================
-- Permissions: allow hive user (used for Sqoop import)
-- ============================================================
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive123';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON scenic.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;

SELECT '=== Business data init done ===' AS status;
SELECT COUNT(*) AS scenic_count FROM t_scenic;
SELECT COUNT(*) AS visitor_count FROM t_visitor;
SELECT COUNT(*) AS consume_count FROM t_consume;
SELECT COUNT(*) AS visit_count FROM t_visit;
SELECT COUNT(*) AS review_count FROM t_review;