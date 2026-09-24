from flask import Flask, render_template, redirect, url_for, session
from flask_cors import CORS
from flask_socketio import SocketIO
from openai import OpenAI
import threading
from routes import  *
from config import Config
from services import start_kafka_consumer
app = Flask(__name__)
app.config.from_object(Config)  # 加载配置类


# 直接在代码中设置配置项
app.config['SECRET_KEY'] = os.environ["SECRET_KEY"]
app.config['MOONSHOT_API_KEY'] = os.environ["MOONSHOT_API_KEY"]
app.config['OPENAI_BASE_URL'] = os.getenv("OPENAI_BASE_URL", "https://api.moonshot.cn/v1")

CORS(app, resources={r"/*": {"origins": "*"}})

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 初始化 OpenAI 客户端
client = OpenAI(
    api_key=app.config['MOONSHOT_API_KEY'],
    base_url=app.config['OPENAI_BASE_URL']
)

client2 = OpenAI(
    api_key=app.config['MOONSHOT_API_KEY'],
    base_url='https://api.moonshot.cn/v1666'
)

# 注册路由
app.add_url_rule('/favicon.ico', view_func=favicon)
app.add_url_rule('/', view_func=home)
app.add_url_rule('/register', view_func=register, methods=['POST'])
app.add_url_rule('/login', view_func=login, methods=['POST'])
app.add_url_rule('/dashboard', view_func=dashboard)
app.add_url_rule('/page1', view_func=page1)
app.add_url_rule('/bytime', view_func=bytime)
app.add_url_rule('/kimi', view_func=kimi)
app.add_url_rule('/chat', view_func=chat, methods=['GET', 'POST'])
app.add_url_rule('/index', view_func=index)
app.add_url_rule('/logout', view_func=logout)
app.add_url_rule('/kimi_analysis', view_func=kimi_analysis)
app.add_url_rule('/get_kimi_analysis', view_func=get_kimi_analysis)

if __name__ == '__main__':
    # 启动 Kafka 消费者线程
    thread = threading.Thread(target=start_kafka_consumer)
    thread.daemon = True
    thread.start()

    socketio.run(
        app,
        host=app.config.get('HOST', '0.0.0.0'),
        port=app.config.get('PORT', 5000),
        debug=app.config.get('DEBUG', False),
    allow_unsafe_werkzeug=True  # 添加这一行
    )