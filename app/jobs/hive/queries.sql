-- =====================================================
-- HiveQL 复杂查询 - 选题十八 智能景区管理系统
-- =====================================================
-- 作业要求：
--   使用HiveQL进行复杂查询，如计算每个景点的日均游客数量、
--   查询高消费游客、对比不同时间段的景点热度等
-- =====================================================

USE scenic_ext;

-- 设置参数让小文件合并、map join 优化
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;
SET hive.groupby.skewindata = true;


-- =====================================================
-- 1. 各景点日均游客数量（Top 10）
-- =====================================================
SELECT
    a.attraction_name,
    COUNT(DISTINCT vr.visitor_id) / COUNT(DISTINCT vr.dt)  AS avg_daily_visitors,
    COUNT(DISTINCT vr.dt)                                 AS active_days,
    COUNT(DISTINCT vr.visitor_id)                         AS total_visitors
FROM ext_t_visit_record vr
JOIN ext_t_attraction a
    ON vr.attraction_id = a.attraction_id
GROUP BY a.attraction_name
ORDER BY avg_daily_visitors DESC
LIMIT 10;


-- =====================================================
-- 2. 高消费游客排行（消费金额 Top 20）
-- =====================================================
SELECT
    v.visitor_id,
    v.visitor_name,
    v.region,
    v.age_group,
    COUNT(c.consumption_id)      AS purchase_count,
    SUM(c.amount)                AS total_amount,
    RANK() OVER (ORDER BY SUM(c.amount) DESC) AS amount_rank
FROM ext_t_consumption c
JOIN ext_t_visitor v
    ON c.visitor_id = v.visitor_id
GROUP BY v.visitor_id, v.visitor_name, v.region, v.age_group
ORDER BY total_amount DESC
LIMIT 20;


-- =====================================================
-- 3. 不同时段的景点热度对比（小时 x 景点）
-- =====================================================
-- 用 RANK 找出每个景点最热的小时段
WITH hourly AS (
    SELECT
        attraction_id,
        HOUR(visit_time)         AS visit_hour,
        COUNT(*)                 AS visit_count
    FROM ext_t_visit_record
    GROUP BY attraction_id, HOUR(visit_time)
)
SELECT
    a.attraction_name,
    h.visit_hour,
    h.visit_count,
    RANK() OVER (PARTITION BY h.attraction_id ORDER BY h.visit_count DESC) AS hour_rank
FROM hourly h
JOIN ext_t_attraction a ON h.attraction_id = a.attraction_id
ORDER BY a.attraction_name, hour_rank
LIMIT 50;


-- =====================================================
-- 4. 不同年龄段的消费偏好对比
-- =====================================================
SELECT
    v.age_group,
    COUNT(DISTINCT c.visitor_id)   AS unique_buyers,
    SUM(c.amount)                  AS total_amount,
    AVG(c.amount)                  AS avg_amount,
    COUNT(c.consumption_id)        AS purchase_count
FROM ext_t_consumption c
JOIN ext_t_visitor v
    ON c.visitor_id = v.visitor_id
GROUP BY v.age_group
ORDER BY total_amount DESC;


-- =====================================================
-- 5. 不同地区的客单价对比
-- =====================================================
SELECT
    v.region,
    COUNT(DISTINCT c.visitor_id)   AS unique_buyers,
    COUNT(c.consumption_id)        AS purchase_count,
    SUM(c.amount)                  AS total_amount,
    SUM(c.amount) / COUNT(DISTINCT c.visitor_id)  AS avg_per_visitor
FROM ext_t_consumption c
JOIN ext_t_visitor v
    ON c.visitor_id = v.visitor_id
GROUP BY v.region
ORDER BY total_amount DESC
LIMIT 20;


-- =====================================================
-- 6. 月度游客消费趋势
-- =====================================================
SELECT
    SUBSTR(dt, 1, 7)               AS month,
    COUNT(DISTINCT visitor_id)     AS monthly_visitors,
    COUNT(consumption_id)          AS monthly_purchases,
    SUM(amount)                    AS monthly_revenue
FROM ext_t_consumption
GROUP BY SUBSTR(dt, 1, 7)
ORDER BY month;


-- =====================================================
-- 7. 周末 vs 工作日对比
-- =====================================================
SELECT
    CASE
        WHEN DAYOFWEEK(TO_DATE(dt)) IN (1, 7)  THEN '周末'
        ELSE '工作日'
    END                              AS day_type,
    COUNT(DISTINCT visitor_id)       AS unique_visitors,
    COUNT(record_id)                 AS total_visits,
    AVG(duration_hours)              AS avg_duration
FROM ext_t_visit_record
GROUP BY
    CASE
        WHEN DAYOFWEEK(TO_DATE(dt)) IN (1, 7)  THEN '周末'
        ELSE '工作日'
    END
ORDER BY day_type;


-- =====================================================
-- 8. 游客消费 Top10 景点（带动消费分析）
-- =====================================================
SELECT
    a.attraction_name,
    COUNT(DISTINCT c.visitor_id)   AS unique_consumers,
    COUNT(c.consumption_id)        AS purchase_count,
    SUM(c.amount)                  AS total_revenue
FROM ext_t_consumption c
JOIN ext_t_attraction a
    ON c.attraction_id = a.attraction_id
GROUP BY a.attraction_name
ORDER BY total_revenue DESC
LIMIT 10;