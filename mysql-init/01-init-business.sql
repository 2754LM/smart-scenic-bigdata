-- ============================================================
-- Smart Scenic BigData Platform - Business Schema Init
-- Auto-runs on first MySQL container start (/docker-entrypoint-initdb.d)
-- Schema-only: 4 tables, Chinese field names, matching data/raw_data/*.csv
-- Data loading is done by scripts/load-csv-to-mysql.py
-- ============================================================

-- Fix MySQL 8.0 init script encoding (entrypoint defaults to latin1)
SET NAMES utf8mb4;
SET character_set_client = utf8mb4;
SET character_set_connection = utf8mb4;

USE scenic;

-- ============================================================
-- 1. 景点表 t_attraction
-- ============================================================
DROP TABLE IF EXISTS t_attraction;
CREATE TABLE t_attraction (
    景点ID   VARCHAR(20)  PRIMARY KEY COMMENT 'Attraction ID',
    景点名称 VARCHAR(100) NOT NULL COMMENT 'Attraction name',
    类型     VARCHAR(50)  COMMENT 'Type: 文化/娱乐/自然/运动',
    位置     VARCHAR(200) COMMENT 'Location code',
    开放时间 VARCHAR(50)  COMMENT 'Open hours',
    INDEX idx_type (类型)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='景点主数据';

-- ============================================================
-- 2. 游客表 t_visitor
-- ============================================================
DROP TABLE IF EXISTS t_visitor;
CREATE TABLE t_visitor (
    游客ID VARCHAR(20)  PRIMARY KEY COMMENT 'Visitor ID',
    姓名   VARCHAR(50)  COMMENT 'Visitor name',
    性别   CHAR(2)      COMMENT 'Gender: 男/女',
    年龄   INT          COMMENT 'Age',
    地区   VARCHAR(50)  COMMENT 'Region',
    INDEX idx_age (年龄),
    INDEX idx_gender (性别)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游客主数据';

-- ============================================================
-- 3. 消费表 t_consumption
-- ============================================================
DROP TABLE IF EXISTS t_consumption;
CREATE TABLE t_consumption (
    消费ID   BIGINT       PRIMARY KEY COMMENT 'Consumption ID',
    时间     DATETIME     COMMENT 'Consumption time',
    游客ID   VARCHAR(20)  NOT NULL COMMENT 'Visitor ID',
    景点ID   VARCHAR(20)  NOT NULL COMMENT 'Attraction ID',
    消费金额 DECIMAL(10,2) COMMENT 'Amount in CNY',
    INDEX idx_v (游客ID),
    INDEX idx_a (景点ID),
    INDEX idx_t (时间),
    INDEX idx_at (景点ID, 时间)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消费流水';

-- ============================================================
-- 4. 游玩记录表 t_visit_record
-- ============================================================
DROP TABLE IF EXISTS t_visit_record;
CREATE TABLE t_visit_record (
    记录ID   BIGINT       PRIMARY KEY COMMENT 'Visit record ID',
    时间     DATETIME     COMMENT 'Visit time',
    游客ID   VARCHAR(20)  NOT NULL COMMENT 'Visitor ID',
    景点ID   VARCHAR(20)  NOT NULL COMMENT 'Attraction ID',
    游玩时长 DECIMAL(5,2) COMMENT 'Duration in hours',
    INDEX idx_v (游客ID),
    INDEX idx_a (景点ID),
    INDEX idx_t (时间),
    INDEX idx_at (景点ID, 时间)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游玩记录';

-- ============================================================
-- Permissions: allow hive user (Hive Metastore + Sqoop)
--   - `hive`/`hive` will own the `hive_metastore` DB schema
--   - also needs SELECT/INSERT on `scenic` for Sqoop import
-- ============================================================
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON scenic.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;

SELECT '=== Schema init done (4 tables, Chinese fields) ===' AS status;
SHOW TABLES FROM scenic;
