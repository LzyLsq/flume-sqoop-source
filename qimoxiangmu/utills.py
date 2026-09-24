import os
from werkzeug.security import generate_password_hash, check_password_hash
import mysql.connector
from mysql.connector import Error

def hash_password(password):
    return generate_password_hash(password)

def verify_password(stored_password, provided_password):
    return check_password_hash(stored_password, provided_password)

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME']
        )
        return conn
    except Error as e:
        print(f"数据库连接错误: {e}")
        return None

def close_db_connection(conn, cursor=None):
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    except Error as e:
        print(f"关闭数据库连接错误: {e}")# Author: [lzy]
# Date: [2025/3/13]
