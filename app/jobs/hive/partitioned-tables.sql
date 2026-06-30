-- ============================================================
-- Hive 分区表 DDL - 智能景区管理系统
-- ============================================================
-- 作业要求：
--   在 HDFS 上使用 Hive 创建表结构，对数据进行分区存储和管理，提高查询效率
--   创建分区表，根据时间、景点等进行分区存储
--
-- 数据来源：
--   Spark clean.py 清洗后写两份：
--     - /scenic/cleaned/         → 非分区版（供 ddl.sql ext_t_*）
--     - /scenic/cleaned_part/    → 分区版（本文件 part_t_*）
--
-- 与 ddl.sql 的区别：
--   ddl.sql          -> /scenic/cleaned/         非分区 Parquet
--   partitioned-tables.sql -> /scenic/cleaned_part/    分区 Parquet
--
-- 执行：
--   docker exec -it hive-server-1 hive -f /opt/jobs/hive/partitioned-tables.sql
-- 或 beeline：
--   beeline -u jdbc:hive2://hive-server-1:10000 -f partitioned-tables.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS scenic_part;
USE scenic_part;

-- ============================================================
-- 1. 消费表（按消费日期 dt 分区）
--    数据：HDFS /scenic/cleaned_part/t_consumption/dt=YYYY-MM-DD/
--    分区列：dt STRING（yyyy-MM-dd）
-- ============================================================
DROP TABLE IF EXISTS scenic_part.part_t_consumption;
CREATE EXTERNAL TABLE scenic_part.part_t_consumption (
    consumption_id BIGINT,
    consume_time   STRING,
    visitor_id     STRING,
    attraction_id  STRING,
    amount         DOUBLE,
    consume_level  STRING,
    consume_date   STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned_part/t_consumption';

-- 自动发现所有 dt=YYYY-MM-DD 分区
MSCK REPAIR TABLE scenic_part.part_t_consumption;

-- ============================================================
-- 2. 游玩记录表（按游玩日期 dt 分区）
--    数据：HDFS /scenic/cleaned_part/t_visit_record/dt=YYYY-MM-DD/
--    分区列：dt STRING（yyyy-MM-dd）
-- ============================================================
DROP TABLE IF EXISTS scenic_part.part_t_visit_record;
CREATE EXTERNAL TABLE scenic_part.part_t_visit_record (
    record_id      BIGINT,
    visit_time     STRING,
    visitor_id     STRING,
    attraction_id  STRING,
    duration_hours DOUBLE,
    visit_date     STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned_part/t_visit_record';

-- 自动发现所有 dt=YYYY-MM-DD 分区
MSCK REPAIR TABLE scenic_part.part_t_visit_record;

-- ============================================================
-- 验证
-- ============================================================
SHOW PARTITIONS scenic_part.part_t_consumption;
SHOW PARTITIONS scenic_part.part_t_visit_record;

SELECT 'part_t_consumption'    AS tbl, COUNT(*) AS n FROM scenic_part.part_t_consumption UNION ALL
SELECT 'part_t_visit_record'  , COUNT(*)        FROM scenic_part.part_t_visit_record;

-- ============================================================
-- 分区查询示例（按时间筛选 + 景点聚合）
-- ============================================================
-- 1) 指定 dt 分区：只扫描该分区的数据
SELECT attraction_id, COUNT(DISTINCT visitor_id) AS visitors
FROM scenic_part.part_t_visit_record
WHERE dt = '2023-06-15'
GROUP BY attraction_id
ORDER BY visitors DESC
LIMIT 10;

-- 2) 多分区范围：扫描 2023-06 月
SELECT dt, COUNT(DISTINCT visitor_id) AS daily_visitors
FROM scenic_part.part_t_visit_record
WHERE dt BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY dt
ORDER BY dt;
