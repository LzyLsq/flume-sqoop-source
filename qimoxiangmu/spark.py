import os
from flask import Flask, render_template, request, jsonify
import pymysql
import logging
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 数据库配置
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "orders7"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db_connection():
    """获取数据库连接"""
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise

@app.route('/')
def index():
    """主页面"""
    return render_template('spark.html')

@app.route('/get_chart_data')
def get_chart_data():
    """获取图表数据"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 获取订单总数
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN is_valid = 'Y' THEN 1 ELSE 0 END) as total_valid,
                    SUM(CASE WHEN is_valid = 'N' THEN 1 ELSE 0 END) as total_invalid
                FROM orders
            """)
            totals = cursor.fetchone()

            # 获取分类数据
            cursor.execute("""
                SELECT 
                    order_category,
                    SUM(CASE WHEN is_valid = 'Y' THEN 1 ELSE 0 END) as valid,
                    SUM(CASE WHEN is_valid = 'N' THEN 1 ELSE 0 END) as invalid
                FROM orders
                WHERE order_category != 'Unknown'
                GROUP BY order_category
                HAVING valid > 0 OR invalid > 0
                LIMIT 10
            """)
            category_data = cursor.fetchall()

        return jsonify({
            'totals': totals,
            'category_data': category_data
        })
    except Exception as e:
        logging.error(f"数据获取失败: {str(e)}")
        return jsonify({"error": "数据服务暂时不可用"}), 500

@app.route('/get_recommendations')
def get_recommendations():
    """获取推荐数据"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Item-Based推荐
            cursor.execute("""
                SELECT main_category, recommended_categories 
                FROM item_based_cf 
                WHERE recommended_categories != ''
                ORDER BY rating_score DESC
                LIMIT 3
            """)
            item_based = [row['main_category'] for row in cursor.fetchall()]

            # User-Based推荐
            # User-Based推荐（修改后的查询）
            cursor.execute("""
                SELECT 
                    recommended_categories, 
                    COUNT(*) AS count,
                    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_based_cf WHERE recommended_categories != '')) AS recommendation_index
                FROM user_based_cf
                WHERE recommended_categories != ''
                GROUP BY recommended_categories
                ORDER BY recommendation_index DESC
                LIMIT 3
            """)
            user_based = [
                {
                    'category': row['recommended_categories'],
                    'count': row['count'],
                    'index': float(row['recommendation_index'])
                } for row in cursor.fetchall()
            ]

            # 离线推荐
            cursor.execute("""
                SELECT order_category 
                FROM category_stats 
                WHERE category_count > 0
                ORDER BY category_count DESC 
                LIMIT 3
            """)
            offline_rec = [row['order_category'] for row in cursor.fetchall()]

        return jsonify({
            'item_based': item_based,
            'user_based': user_based,
            'offline': offline_rec
        })
    except Exception as e:
        logging.error(f"推荐数据获取失败: {str(e)}")
        return jsonify({"error": "推荐服务不可用"}), 500

def _parse_recommendations(data):
    """解析推荐数据"""
    return list({c for row in data
                 for c in row.get('recommended_categories', '').split(',')
                 if c.strip() != ''})[:3]

@app.route('/search_category', methods=['POST'])
def search_category():
    """分类搜索"""
    try:
        category = request.form.get('category', '').strip()
        if not category:
            return jsonify({"error": "请输入分类名称"}), 400

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    category_b as related_category, 
                    association_count
                FROM category_category_matrix
                WHERE category_a = %s
                AND association_count > 0
                ORDER BY association_count DESC
                LIMIT 3
            """, (category,))
            results = cursor.fetchall()

        return jsonify({'results': results})
    except Exception as e:
        logging.error(f"搜索失败: {str(e)}")
        return jsonify({"error": "搜索服务异常"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5022, debug=True)