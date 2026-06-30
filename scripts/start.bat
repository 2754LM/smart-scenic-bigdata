@echo off
echo ==========================================================
echo  DEPRECATED: please use start-containers.bat
echo ==========================================================
echo(
echo  This script used to do everything. Now there are two scripts:
echo(
echo     scripts\start-containers.bat   (just the 15 Docker containers)
echo     scripts\start-app.bat          (data pipeline: CSV -^> MySQL -^> HDFS -^> Hive -^> ML)
echo(
echo  Run them in order.
echo(
pause
