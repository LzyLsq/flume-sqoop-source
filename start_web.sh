#!/bin/bash
# 启动 Flask Web 应用（后台运行）
cd ~/Desktop/Flume_Sqoop_Project/qimoxiangmu
set -a; source .env; set +a
nohup python3 app.py > /tmp/flask_app.log 2>&1 &
echo "Flask 已启动，PID: $!"
sleep 3
grep -viE "urllib3|warnings.warn" /tmp/flask_app.log | head -8
