import os

class Config:
    SECRET_KEY = os.environ['SECRET_KEY']
    DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    HOST = os.getenv('FLASK_HOST', '127.0.0.1')
    PORT = int(os.getenv('FLASK_PORT', 5000))

    # 数据库配置（从环境变量读取，无默认值）
    DB_CONFIG = {
        'host': os.environ['DB_HOST'],
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASSWORD'],
        'database': os.environ['DB_NAME'],
    }

    # Kafka 配置
    KAFKA_BROKER = os.environ['KAFKA_BROKER']
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'processed_orders')

    # OpenAI API 配置
    MOONSHOT_API_KEY = os.environ["MOONSHOT_API_KEY"]
    OPENAI_BASE_URL= os.getenv("OPENAI_BASE_URL", "https://api.moonshot.cn/v1")