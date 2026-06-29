-- =====================================================
-- Hive 视图 - 选题十八 智能景区管理系统
-- =====================================================
-- 作业要求：
--   创建视图，简化复杂查询，提高查询效率
-- =====================================================

USE scenic_ext;

-- 删除已存在的视图
DROP VIEW IF EXISTS v_attraction_summary;
DROP VIEW IF EXISTS v_daily_visits;
DROP VIEW IF EXISTS v_high_value_visitors;
DROP VIEW IF EXISTS v_attraction_hourly_heat;


-- =====================================================
-- 1. 景点汇总视图（每个景点的统计信息）
-- =====================================================
CREATE VIEW v_attraction_summary AS
SELECT
    a.attraction_id,
    a.attraction_name,
    a.attraction_type,
    a.location,
    COALESCE(SUM(c.amount), 0)                                AS total_revenue,
    COALESCE(COUNT(DISTINCT vr.visitor_id), 0)                AS total_visitors,
    COALESCE(AVG(vr.duration_hours), 0)                       AS avg_duration_hours,
    COALESCE(COUNT(vr.record_id), 0)                          AS total_visits
FROM ext_t_attraction a
LEFT JOIN ext_t_consumption c
    ON a.attraction_id = c.attraction_id
LEFT JOIN ext_t_visit_record vr
    ON a.attraction_id = vr.attraction_id
GROUP BY
    a.attraction_id, a.attraction_name, a.attraction_type, a.location;


-- =====================================================
-- 2. 每日游客量视图（按时段聚合）
-- =====================================================
CREATE VIEW v_daily_visits AS
SELECT
    dt,
    attraction_id,
    COUNT(DISTINCT visitor_id) AS daily_visitors,
    COUNT(record_id)           AS daily_visit_records,
    AVG(duration_hours)        AS avg_duration_hours
FROM ext_t_visit_record
GROUP BY dt, attraction_id;


-- =====================================================
-- 3. 高消费游客视图（消费金额 >= 1000）
-- =====================================================
CREATE VIEW v_high_value_visitors AS
SELECT
    c.visitor_id,
    v.visitor_name,
    v.region,
    v.age_group,
    COUNT(c.consumption_id)      AS purchase_count,
    SUM(c.amount)                AS total_amount,
    AVG(c.amount)                AS avg_amount
FROM ext_t_consumption c
JOIN ext_t_visitor v
    ON c.visitor_id = v.visitor_id
GROUP BY c.visitor_id, v.visitor_name, v.region, v.age_group
HAVING SUM(c.amount) >= 1000;


-- =====================================================
-- 4. 景点时段热度视图（按小时统计）
-- =====================================================
CREATE VIEW v_attraction_hourly_heat AS
SELECT
    attraction_id,
    HOUR(visit_time)            AS visit_hour,
    COUNT(record_id)            AS visit_count,
    COUNT(DISTINCT visitor_id)  AS unique_visitors
FROM ext_t_visit_record
GROUP BY attraction_id, HOUR(visit_time);


-- 验证
SHOW VIEWS IN scenic_ext;

SELECT * FROM v_attraction_summary LIMIT 5;
SELECT * FROM v_daily_visits       LIMIT 5;
SELECT * FROM v_high_value_visitors LIMIT 5;
SELECT * FROM v_attraction_hourly_heat LIMIT 5;