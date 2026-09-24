import os
import mysql.connector

try:
    connection = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )
    print("连接成功！")
except mysql.connector.Error as err:
    print(f"连接失败：{err}")
