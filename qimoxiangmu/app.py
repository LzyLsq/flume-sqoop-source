import os
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from flask_cors import CORS
import logging
from werkzeug.security import generate_password_hash, check_password_hash
import mysql.connector
from mysql.connector import Error
from flask import Flask, render_template, jsonify  # Flask 用于创建 Web 应用，render_template 用于渲染 HTML 模板
from flask_socketio import SocketIO  # 用于实现 WebSocket 实时通信
import json  # 用于处理 JSON 数据
from kafka import KafkaConsumer  # Kafka 消费者模块，用于消费 Kafka 主题数据
import threading  # 用于创建多线程
from flask_cors import CORS  # 用于解决跨域问题
import requests  # 用于发送 HTTP 请求到 Kimi API
import os

from openai import OpenAI
app = Flask(__name__)

# 安全修复：SECRET_KEY 必须来自环境变量，不再提供可猜测的默认值
# 缺少该变量时应用直接拒绝启动，避免 Session 被伪造
if not os.environ.get("SECRET_KEY"):
    raise RuntimeError(
        "环境变量 SECRET_KEY 未设置。请生成随机值并配置后再启动："
        "python3 -c \"import secrets; print(secrets.token_hex(32))\" "
        "然后 export SECRET_KEY=<生成的值>"
    )
app.secret_key = os.environ["SECRET_KEY"]

# 安全加固：限制跨域来源，默认仅允许本机访问
CORS(app, resources={
    r"/*": {"origins": os.environ.get("CORS_ORIGINS", "http://127.0.0.1,http://localhost").split(",")}
})

# 全站响应安全头（替代原先仅设置在单个路由上的写法）
@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KimiAPI')

db_config = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.environ['DB_NAME'],
}

client2 = OpenAI(
    api_key=os.environ["MOONSHOT_API_KEY"],
    base_url=os.getenv("OPENAI_BASE_URL", "https://api.moonshot.cn/v1")
)

# 设置 OpenAI API 的相关参数
api_key = os.environ["MOONSHOT_API_KEY"]
base_url = "https://api.moonshot.cn/v1"
client = OpenAI(api_key=api_key, base_url=base_url)

# 2. 配置 Flask-SocketIO
_cors_origins = os.environ.get("CORS_ORIGINS", "http://127.0.0.1,http://localhost").split(",")
socketio = SocketIO(app, cors_allowed_origins=_cors_origins, async_mode='threading')  # 初始化 SocketIO，跨域来源已收窄为白名单

# 3. 配置 Kafka 服务信息
KAFKA_BROKER = os.environ['KAFKA_BROKER']  # Kafka 代理服务器地址（从环境变量读取）
KAFKA_TOPIC = 'processed_orders'  # 需要订阅的 Kafka 主题名称

# 4. 初始化全局变量，用于存储累计的订单统计数据
order_stats = {'valid': 0, 'invalid': 0}  # 用于存储有效和无效订单数量
order_category_stats = {}  # 用于存储订单分类统计数据
order_number_stats = {}  # 用于存储订单数量统计数据
kimi_analysis_result = {}  # 用于存储 Kimi 的分析结果

# 5. 提供 favicon.ico 文件的路由
@app.route('/favicon.ico')  # 设置路由用于提供 favicon.ico 文件
def favicon():
    return app.send_static_file('favicon.ico')  # 返回静态资源 favicon.ico

# 6. 启动 Kafka 消费者服务，监听 Kafka 主题数据
def start_kafka_consumer():
    """
    创建 Kafka 消费者实例并持续监听指定主题的消息。
    每当接收到新的消息时，更新统计数据并将数据通过 WebSocket 推送到前端。
    """
    # 创建 Kafka 消费者实例
    consumer = KafkaConsumer(
        KAFKA_TOPIC,  # 订阅的主题名称
        bootstrap_servers=[KAFKA_BROKER],  # Kafka 代理服务器地址
        group_id='my-group',  # 消费者组 ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # 将消息内容解码为 JSON 格式
    )

    print("Kafka 消费者启动中...")

    # 循环监听主题消息
    for message in consumer:
        data = message.value  # 获取消息内容
        print(f"接到 Kafka 消息: {data}")

        # 更新全局统计数据
        update_order_stats(data.get('order_stats', {}))  # 更新订单状态统计
        update_order_category_stats(data.get('order_category_stats', []))  # 更新订单分类统计
        update_order_number_stats(data.get('order_number_stats', []))  # 更新订单数量统计

        # 将更新后的统计数据通过 WebSocket 推送到前端
        emit_data_to_frontend()

        # 将数据发送到 Kimi API 进行分析
        send_to_kimi_api()

# 7. 更新订单统计数据（函数的定义）
def update_order_stats(new_stats):
    """
    累加全局订单统计数据，包括有效订单和无效订单数量。
    :param new_stats: 新的订单统计数据，包含有效和无效订单数量
    """
    order_stats['valid'] += new_stats.get('valid', 0)  # 累加有效订单数量
    order_stats['invalid'] += new_stats.get('invalid', 0)  # 累加无效订单数量

def update_order_category_stats(new_category_stats):
    """
    累加订单分类统计数据，按分类更新各类型订单的统计值。
    :param new_category_stats: 新的订单分类统计数据
    """
    for category_stats in new_category_stats:  # 遍历每个分类统计字典
        for category, stats in category_stats.items():  # 遍历分类及其统计数据
            if category not in order_category_stats:
                # 如果分类不存在，则初始化分类统计数据
                order_category_stats[category] = {'Y': 0, 'N': 0, 'total_quantity': 0}
            # 累加分类数据
            order_category_stats[category]['Y'] += stats.get('Y', 0)  # 累加有效订单数量
            order_category_stats[category]['N'] += stats.get('N', 0)  # 累加无效订单数量
            order_category_stats[category]['total_quantity'] += stats.get('total_quantity', 0)  # 累加总数量

def update_order_number_stats(new_number_stats):
    """
    累加订单数量统计数据，按订单名称更新数量统计。
    :param new_number_stats: 新的订单数量统计数据
    """
    for number_stats in new_number_stats:  # 遍历每个订单统计字典
        for order_name, stats in number_stats.items():  # 遍历订单名称及其统计数据
            if order_name not in order_number_stats:
                # 如果订单名称不存在，则初始化订单数量统计数据
                order_number_stats[order_name] = {'Y': 0, 'N': 0}
            # 累加订单数量
            order_number_stats[order_name]['Y'] += stats.get('Y', 0)  # 累加有效订单数量
            order_number_stats[order_name]['N'] += stats.get('N', 0)  # 累加无效订单数量

# 8. 将数据推送到前端
def emit_data_to_frontend():
    """
    将更新后的统计数据通过 WebSocket 推送到前端。
    """
    socketio.emit('update_data', {
        'order_stats': order_stats,   # 发送订单状态统计
        'order_category_stats': order_category_stats,   # 发送订单分类统计
        'order_number_stats': order_number_stats   # 发送订单数量统计
    })
    print("发送到前端的数据：", {
        'order_stats': order_stats,
        'order_category_stats': order_category_stats,
        'order_number_stats': order_number_stats
    })

# 9. 将数据发送 Kimi API
def send_to_kimi_api():
    """
    将统计数据发送到 Kimi API 进行分析，并将分析结果存储以便前端获取。
    """
    data = {
        'order_stats': order_stats,
        'order_category_stats': order_category_stats,
        'order_number_stats': order_number_stats
    }
    try:
        # 使用 OpenAI SDK 发送请求
        completion = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system", "content": "你是Kimi，由Moonshot AI提供的智能助手,下面我会每隔一段时间给你发送一串数据，请对这些数据中的订单类别进行实时分析根据每一次发送的数据找出最优生产建议..."},
                {"role": "user", "content": json.dumps(data)}
            ],
            temperature=0.3,
        )
        ai_response = completion.choices[0].message.content
        # 存储分析结果以便前端获取
        global kimi_analysis_result
        kimi_analysis_result = ai_response
        print("Kimi 分析结果：", ai_response)

        # 将分析结果通过 WebSocket 推送到前端
        socketio.emit('update_data', {
            'kimi_analysis_result': ai_response
        })
    except Exception as e:
        print("Kimi API 请求异常：", str(e))

# 10. 获取 Kimi 分析结果的路由
@app.route('/get_kimi_analysis')
def get_kimi_analysis():
    """
    前端通过该路由获取 Kimi 的分析结果。
    """
    return jsonify(kimi_analysis_result)

# 11. 设置路由渲染 index.html 页面

# 12. 设置路由渲染 Kimi 分析结果页面
@app.route('/kimi_analysis')   # 设置 Kimi 分析结果页面路由
def kimi_analysis():
    """
    设置 Kimi 分析结果页面路由，渲染前端页面 kimi_analysis.html。
    :return: 渲染后的 HTML 页面
    """
    return render_template('kimi_analysis.html')   # 用于返回前端 HTML 模板

# 13. 启动 Kafka 消费者线程
def start_consumer_thread():
    """
    创建一个线程运行 Kafka 消费者，保证 Flask 应用不会被阻塞。
    """
    thread = threading.Thread(target=start_kafka_consumer)   # 创建线程，目标函数为 start_kafka_consumer
    thread.daemon = True   # 设置为守护线程，主线程退出时子线程自动结束
    thread.start()   # 启动线程
    print("消费者线程已启动")

@app.route('/')
def home():
    return render_template('login.html')
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')

    hashed_password = generate_password_hash(password)

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        existing_user = cursor.fetchone()

        if existing_user:
            return jsonify({'success': False, 'message': '用户名已存在，请选择其他用户名。'})

        cursor.execute(
            "INSERT INTO users (username, password, email) VALUES (%s, %s, %s)",
            (username, hashed_password, email)
        )
        conn.commit()
        return jsonify({'success': True, 'message': '注册成功！'})
    except Error as e:
        return jsonify({'success': False, 'message': f"数据库错误: {e}"})
    finally:
        cursor.close()
        conn.close()

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

        if user and check_password_hash(user['password'], password):
            session['logged_in'] = True
            session['username'] = user['username']
            return jsonify({'success': True, 'message': '登录成功！'})
        else:
            return jsonify({'success': False, 'message': '用户名或密码错误，请检查后重试。'})
    except Error as e:
        return jsonify({'success': False, 'message': f"数据库错误: {e}"})
    finally:
        cursor.close()
        conn.close()

@app.route('/dashboard')
def dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('dashboard.html')

@app.route('/page1')
def page1():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('kimi_analysis.html')

@app.route('/bytime')
def bytime():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('bytime.html')

@app.route('/kimi')
def kimi():
    if not session.get('logged_in'):
        logger.info("用户未登录，重定向到首页")
        return redirect(url_for('home'))
    logger.info("访问 kimi.html")
    return render_template('kimi.html')

@app.route('/chat', methods=['POST'])
def chat():
    # 修复：原实现 GET 也会进入函数体再被 415 拒绝，语义错误；
    #       且参数非法时返回 404（应为 400），安全响应头也重复设置在此处。
    if not request.is_json:
        return jsonify({'error': '请求必须是 application/json'}), 415

    data = request.get_json(silent=True) or {}
    user_question = data.get('question', '')

    if not isinstance(user_question, str) or len(user_question.strip()) < 2:
        return jsonify({'error': 'question 字段缺失或内容过短（至少 2 个字符）'}), 400

    try:
        completion = client2.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system", "content": "你是AI，由Moonshot AI提供的智能助手我将询问你一些关于订单方面的问题..."},
                {"role": "user", "content": user_question}
            ],
            temperature=0.3,
        )
        return jsonify({'answer': completion.choices[0].message.content})
    except Exception as e:
        logger.error(f"API调用失败: {str(e)}")
        return jsonify({'error': '内部服务错误'}), 500

@app.route('/show')
def show():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('show.html')

@app.route('/First')
def First():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('First.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('username', None)
    return redirect(url_for('home'))

if __name__ == '__main__':
    start_consumer_thread()
    app.run(host='0.0.0.0',
            port=int(os.getenv("FLASK_PORT", 5000)),

            debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
            threaded=True)