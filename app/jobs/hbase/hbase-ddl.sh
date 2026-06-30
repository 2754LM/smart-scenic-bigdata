#!/bin/bash
# ============================================================
# HBase 表结构 - 智能景区管理系统
# ============================================================
# 作业要求：
#   在 HBase 中存储实时游玩记录数据，并进行快速查询
#   创建表结构，包括列族和列，如：时间、游客ID、景点ID、游玩时长
#
# 三张表：
#   1. scenic_reviews     - 评论表（demo 用）
#   2. scenic_realtime    - 实时事件（Kafka 入口/出口事件）
#   3. scenic_visit_record - 游玩记录（时间/游客ID/景点ID/游玩时长）← 本作业要求
#
# 执行：
#   docker exec hbase-master hbase shell /opt/jobs/hbase/hbase-ddl.sh
# 或本地：
#   docker exec hbase-master hbase shell < hbase-ddl.sh
# ============================================================

# 1. 评论表（demo）
create "scenic_reviews", "cf"

# 2. 实时事件表（Kafka consumer 写入 entry/exit）
create "scenic_realtime", "cf"

# 3. 游玩记录表（作业要求：时间/游客ID/景点ID/游玩时长）
#    row_key 设计：V{visitor_id}_{visit_time}（按游客前缀查）
#    列族 cf：
#       cf:visit_time      STRING   游玩时间
#       cf:attraction_id   STRING   景点ID
#       cf:duration_hours  STRING   游玩时长（小时）
disable "scenic_visit_record"
drop "scenic_visit_record"
create "scenic_visit_record", "cf"

# 4. 游客画像表（按游客聚合统计）
#    row_key：V{visitor_id}（8 位补零）
#    列族 stats：
#       stats:total_visits      BIGINT  总游玩次数
#       stats:total_duration    DOUBLE  总游玩时长
#       stats:last_attraction   STRING  最近去的景点
#       stats:last_visit_time   STRING  最近游玩时间
disable "scenic_visitor_profile"
drop "scenic_visitor_profile"
create "scenic_visitor_profile", "stats"

# 5. 景点热度表（按景点聚合统计）
#    row_key：A{attraction_id}（4 位补零）
#    列族 stats：
#       stats:total_visitors    BIGINT  累计游客数
#       stats:total_duration    DOUBLE  累计游玩时长
#       stats:last_visit_time   STRING  最近入园时间
disable "scenic_attraction_heat"
drop "scenic_attraction_heat"
create "scenic_attraction_heat", "stats"

# 验证
list
describe "scenic_visit_record"
describe "scenic_visitor_profile"
describe "scenic_attraction_heat"

echo "=== HBase DDL Done ==="
