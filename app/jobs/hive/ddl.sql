-- =====================================================
-- Hive 外部表 DDL - 智能景区管理系统
-- =====================================================
-- 执行：hive -f /opt/jobs/hive/ddl.sql
-- 或 beeline -u jdbc:hive2://hive-server-1:10000 -f /opt/jobs/hive/ddl.sql
-- =====================================================

CREATE DATABASE IF NOT EXISTS scenic_ext;

-- 1. 景点维表
DROP TABLE IF EXISTS scenic_ext.ext_t_attraction;
CREATE EXTERNAL TABLE scenic_ext.ext_t_attraction (
    attraction_id   STRING,
    attraction_name STRING,
    attraction_type STRING,
    location        STRING,
    open_time       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_attraction';

-- 2. 游客维表
DROP TABLE IF EXISTS scenic_ext.ext_t_visitor;
CREATE EXTERNAL TABLE scenic_ext.ext_t_visitor (
    visitor_id   STRING,
    visitor_name STRING,
    gender       STRING,
    age          INT,
    region       STRING,
    age_group    STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_visitor';

-- 3. 消费事实表
DROP TABLE IF EXISTS scenic_ext.ext_t_consumption;
CREATE EXTERNAL TABLE scenic_ext.ext_t_consumption (
    consumption_id BIGINT,
    consume_time   STRING,
    visitor_id     STRING,
    attraction_id  STRING,
    amount         DOUBLE,
    consume_level  STRING,
    consume_date   STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_consumption';

-- 4. 游玩记录表
DROP TABLE IF EXISTS scenic_ext.ext_t_visit_record;
CREATE EXTERNAL TABLE scenic_ext.ext_t_visit_record (
    record_id      BIGINT,
    visit_time     STRING,
    visitor_id     STRING,
    attraction_id  STRING,
    duration_hours DOUBLE,
    visit_date     STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_visit_record';

    -- 验证 (避免 MR 引擎跑全表扫描，这里只 SHOW TABLES，不 SELECT)
    SHOW TABLES IN scenic_ext;