#!/usr/bin/env bash
# 将 MySQL 业务库的 用户协同过滤推荐结果 表导入 Hive。
#
# 安全说明：MySQL 连接串 / 账号 / 密码一律通过环境变量注入，不写入仓库。
# 使用前先导出：
#   export MYSQL_HOST=39.105.42.89
#   export DB_NAME=orders7
#   export DB_USER=<你的用户名>
#   export DB_PASSWORD=<你的密码>
# 提示：--password 会出现在进程列表中，生产环境建议改用 Sqoop 的
#       --password-file <本地凭证文件> 方式。
set -euo pipefail

: "${MYSQL_HOST:?请先设置环境变量 MYSQL_HOST}"
: "${DB_USER:?请先设置环境变量 DB_USER}"
: "${DB_PASSWORD:?请先设置环境变量 DB_PASSWORD}"
DB_NAME="${DB_NAME:-orders7}"

sqoop import \
  --connect "jdbc:mysql://${MYSQL_HOST}:3306/${DB_NAME}" \
  --username "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --table user_based_cf \
  --hive-import \
  --hive-database "${DB_NAME}" \
  --hive-table user_based_cf \
  --num-mappers 1 \
  --fields-terminated-by '\t'
