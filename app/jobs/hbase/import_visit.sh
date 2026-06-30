#!/bin/bash
# ============================================================
# HBase 批量导入游玩记录 - 智能景区管理系统
# ============================================================
# 把 Spark write_visit_to_hbase.py 生成的 HDFS put 脚本批量灌进 HBase
#
# 数据源：
#   HDFS /tmp/hbase_import/visit/{scenic_visit_record,scenic_visitor_profile,scenic_attraction_heat}/
#
# 执行：
#   docker exec hbase-master bash /opt/jobs/hbase/import_visit.sh
# ============================================================
set -e

HDFS_BASE="/tmp/hbase_import/visit"
LOCAL_TMP="/tmp/hbase_import_local"

echo "=== 1. 拉取 HDFS put 脚本到本地 ==="
mkdir -p "${LOCAL_TMP}"
for tbl in scenic_visit_record scenic_visitor_profile scenic_attraction_heat; do
    echo "  -- ${tbl} --"
    rm -rf "${LOCAL_TMP}/${tbl}"
    mkdir -p "${LOCAL_TMP}/${tbl}"
    # HDFS 上的 Spark 输出是 part-XXXXX 文件
    docker exec hadoop-namenode hdfs dfs -get "${HDFS_BASE}/${tbl}" "${LOCAL_TMP}/" 2>/dev/null || \
    hdfs dfs -get "${HDFS_BASE}/${tbl}" "${LOCAL_TMP}/" 2>/dev/null || \
    cp -r /tmp/hbase_import/visit/${tbl}/* "${LOCAL_TMP}/${tbl}/" 2>/dev/null
done

# 合并所有 part 文件
echo "=== 2. 合并 part 文件 ==="
for tbl in scenic_visit_record scenic_visitor_profile scenic_attraction_heat; do
    cat "${LOCAL_TMP}/${tbl}"/part-* > "${LOCAL_TMP}/${tbl}.txt" 2>/dev/null || true
    LINES=$(wc -l < "${LOCAL_TMP}/${tbl}.txt" 2>/dev/null || echo 0)
    echo "  ${tbl}: ${LINES} 行 put 命令"
done

# 执行 HBase put
echo "=== 3. 灌入 HBase ==="
for tbl in scenic_visit_record scenic_visitor_profile scenic_attraction_heat; do
    if [ -s "${LOCAL_TMP}/${tbl}.txt" ]; then
        echo "  -- importing ${tbl} --"
        hbase shell "${LOCAL_TMP}/${tbl}.txt" 2>&1 | tail -3
    fi
done

echo ""
echo "=== 4. 验证 ==="
hbase shell <<'EOF'
count "scenic_visit_record"
count "scenic_visitor_profile"
count "scenic_attraction_heat"
EOF

echo "=== HBase import_visit done ==="
