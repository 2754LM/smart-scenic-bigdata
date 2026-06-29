-- ============================================================
-- Smart Scenic BigData Platform - Full Initialization
-- Table structures designed to match CSV columns exactly
-- ============================================================

CREATE DATABASE IF NOT EXISTS scenic;
USE scenic;

-- Attractions table (matches attractions.csv)
CREATE TABLE IF NOT EXISTS t_scenic (
    scenic_id     INT PRIMARY KEY COMMENT 'Scenic ID',
    scenic_name   VARCHAR(100) NOT NULL COMMENT 'Scenic name',
    scenic_type   VARCHAR(50) COMMENT 'Type',
    location      VARCHAR(200) COMMENT 'Location',
    open_time     VARCHAR(50) COMMENT 'Open hours'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Visitors table (matches visitors.csv)
CREATE TABLE IF NOT EXISTS t_visitor (
    visitor_id   INT PRIMARY KEY COMMENT 'Visitor ID',
    name         VARCHAR(50) NOT NULL COMMENT 'Name',
    gender       CHAR(2) COMMENT 'Gender',
    age          INT COMMENT 'Age',
    region       VARCHAR(100) COMMENT 'Region'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Consumption records table (matches consumption.csv)
CREATE TABLE IF NOT EXISTS t_consume (
    consume_id    INT PRIMARY KEY COMMENT 'Consumption ID',
    consume_time  DATETIME NOT NULL COMMENT 'Time',
    visitor_id    INT NOT NULL COMMENT 'Visitor ID',
    scenic_id     INT NOT NULL COMMENT 'Scenic ID',
    amount        DECIMAL(10,2) DEFAULT 0.00 COMMENT 'Amount'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Visit records table (matches visit_records.csv)
CREATE TABLE IF NOT EXISTS t_visit (
    record_id      INT PRIMARY KEY COMMENT 'Record ID',
    visit_time     DATETIME NOT NULL COMMENT 'Time',
    visitor_id     INT NOT NULL COMMENT 'Visitor ID',
    scenic_id      INT NOT NULL COMMENT 'Scenic ID',
    duration_hours DECIMAL(5,2) DEFAULT 0.00 COMMENT 'Duration (hours)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT '✅ Table structures created successfully' AS status;

-- Create hive user and grant privileges
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive123';
GRANT ALL PRIVILEGES ON scenic.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;

SELECT '✅ Hive user created successfully' AS status;

-- ============================================================
-- Import CSV data (one-to-one mapping, fully matched)
-- ============================================================

LOAD DATA INFILE '/var/lib/mysql-files/attractions.csv'
INTO TABLE t_scenic
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT CONCAT('✅ Attractions imported: ', ROW_COUNT(), ' rows') AS result;

LOAD DATA INFILE '/var/lib/mysql-files/visitors.csv'
INTO TABLE t_visitor
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT CONCAT('✅ Visitors imported: ', ROW_COUNT(), ' rows') AS result;

LOAD DATA INFILE '/var/lib/mysql-files/consumption.csv'
INTO TABLE t_consume
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT CONCAT('✅ Consumption records imported: ', ROW_COUNT(), ' rows') AS result;

LOAD DATA INFILE '/var/lib/mysql-files/visit_records.csv'
INTO TABLE t_visit
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT CONCAT('✅ Visit records imported: ', ROW_COUNT(), ' rows') AS result;

-- Data validation
SELECT '============================' AS '';
SELECT '📊 Data Import Statistics' AS '';
SELECT '============================' AS '';
SELECT CONCAT('Attractions: ', COUNT(*)) FROM t_scenic;
SELECT CONCAT('Visitors: ', COUNT(*)) FROM t_visitor;
SELECT CONCAT('Consumption records: ', COUNT(*)) FROM t_consume;
SELECT CONCAT('Visit records: ', COUNT(*)) FROM t_visit;

SELECT '=== ✅ All initialization completed ===' AS status;