-- ============================================================
-- Smart Scenic BigData - Hive External Tables on Sqoop Raw Data
-- ============================================================
-- 用途：在 Hive 里建 4 张外部表，指向 Sqoop 导入的原始数据
--       路径：/scenic/sqoop/{t_attraction,t_visitor,t_consumption,t_visit_record}
--
-- 数据流向：
--   MySQL 业务库  --(Sqoop import)-->  HDFS /scenic/sqoop/  --(本 DDL)-->  Hive 外表
--
-- 与 ddl.sql 的区别：
--   ddl.sql           →  指向 /scenic/cleaned/ 的 Parquet 表（ETL 之后）
--   本文件            →  指向 /scenic/sqoop/  的 TEXTFILE 表（原始数据）
--
-- 执行：
--   docker exec -it hive-server-1 hive -f /opt/jobs/hive/sqoop-raw-tables.sql
-- 或 beeline：
--   beeline -u jdbc:hive2://hive-server-1:10000 -f sqoop-raw-tables.sql
-- ============================================================

-- 默认 database 即为 hive_metastore 启动时的库
-- 如果需要切换：
-- CREATE DATABASE IF NOT EXISTS scenic_sqoop;
-- USE scenic_sqoop;

-- ============================================================
-- 1. 景点表
--    MySQL 字段顺序：景点ID, 景点名称, 类型, 位置, 开放时间
--    Sqoop 导出无表头，按 MySQL 字段顺序写入
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS t_attraction (
    scenic_id     INT,
    scenic_name   STRING,
    scenic_type   STRING,
    location      STRING,
    open_time     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/scenic/sqoop/t_attraction';

-- ============================================================
-- 2. 游客表
--    MySQL 字段顺序：游客ID, 姓名, 性别, 年龄, 地区
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS t_visitor (
    visitor_id   INT,
    name         STRING,
    gender       STRING,
    age          INT,
    region       STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/scenic/sqoop/t_visitor';

-- ============================================================
-- 3. 消费记录表
--    MySQL 字段顺序：消费ID, 时间, 游客ID, 景点ID, 消费金额
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS t_consumption (
    consume_id    INT,
    consume_time  STRING,
    visitor_id    INT,
    scenic_id     INT,
    amount        DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/scenic/sqoop/t_consumption';

-- ============================================================
-- 4. 游玩记录表
--    MySQL 字段顺序：记录ID, 时间, 游客ID, 景点ID, 游玩时长
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS t_visit_record (
    record_id      INT,
    visit_time     STRING,
    visitor_id     INT,
    scenic_id      INT,
    duration_hours DECIMAL(5,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/scenic/sqoop/t_visit_record';

-- ============================================================
-- 验证
-- ============================================================
SHOW TABLES;

SELECT 't_attraction'    AS tbl, COUNT(*) AS n FROM t_attraction UNION ALL
SELECT 't_visitor'      , COUNT(*)        FROM t_visitor UNION ALL
SELECT 't_consumption'  , COUNT(*)        FROM t_consumption UNION ALL
SELECT 't_visit_record' , COUNT(*)        FROM t_visit_record;
