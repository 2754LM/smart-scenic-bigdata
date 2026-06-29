-- =====================================================
-- Hive 分区表 DDL - 选题十八 智能景区管理系统
-- =====================================================
-- 作业要求：
--   在HDFS上使用Hive创建表结构，对数据进行分区存储和管理，提高查询效率
--   - 创建分区表，根据时间、景点等进行分区存储
--   - 创建视图，简化复杂查询，提高查询效率
--   - 使用HiveQL进行复杂查询
--
-- 执行方式（在 hive-server 容器内）：
--   hive -f /opt/jobs/hive/ddl.sql
--   或 beeline -u jdbc:hive2://hive-server-1:10000 -f /opt/jobs/hive/ddl.sql
-- =====================================================

-- 切换数据库
USE scenic_ext;

-- 如果表已存在先删（重建场景）
DROP TABLE IF EXISTS ext_t_consumption;
DROP TABLE IF EXISTS ext_t_visit_record;
DROP TABLE IF EXISTS ext_t_attraction;
DROP TABLE IF EXISTS ext_t_visitor;


-- =====================================================
-- 1. 景点维表（不分区，数据量小）
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ext_t_attraction (
    attraction_id   STRING,
    attraction_name STRING,
    attraction_type STRING,
    location        STRING,
    open_time       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_attraction';


-- =====================================================
-- 2. 游客维表（不分区，数据量中等）
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ext_t_visitor (
    visitor_id   STRING,
    visitor_name STRING,
    gender       STRING,
    age          INT,
    region       STRING,
    age_group    STRING  -- 派生字段
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_visitor';


-- =====================================================
-- 3. 消费事实表（按日期分区：dt）
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ext_t_consumption (
    consumption_id BIGINT,
    consume_time   TIMESTAMP,
    visitor_id     STRING,
    attraction_id  STRING,
    amount         DOUBLE,
    consume_level  STRING  -- 派生字段
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/hive/ext_t_consumption';

-- 从清洗后的数据加载并按 consume_date 分区
-- （注意：Spark clean.py 已将数据按 parquet 写入 HDFS，
--    这里用 MSCK REPAIR TABLE 自动发现分区，或用 ALTER TABLE 手动加）
MSCK REPAIR TABLE ext_t_consumption;


-- =====================================================
-- 4. 游玩记录事实表（按日期 + 景点双分区）
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ext_t_visit_record (
    record_id      BIGINT,
    visit_time     TIMESTAMP,
    visitor_id     STRING,
    attraction_id  STRING,
    duration_hours DOUBLE
)
PARTITIONED BY (dt STRING, attraction_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/hive/ext_t_visit_record';

MSCK REPAIR TABLE ext_t_visit_record;


-- =====================================================
-- 验证
-- =====================================================
SHOW TABLES IN scenic_ext;

SELECT 'ext_t_attraction'      AS tbl, COUNT(*) AS n FROM ext_t_attraction UNION ALL
SELECT 'ext_t_visitor'        , COUNT(*)        FROM ext_t_visitor UNION ALL
SELECT 'ext_t_consumption'    , COUNT(*)        FROM ext_t_consumption UNION ALL
SELECT 'ext_t_visit_record'   , COUNT(*)        FROM ext_t_visit_record;