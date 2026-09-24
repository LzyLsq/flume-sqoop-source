from kafka import KafkaConsumer
import json
from flask_socketio import SocketIO
from config import Config
import threading
from openai import OpenAI

client = OpenAI(
    api_key=Config.MOONSHOT_API_KEY,
    base_url=Config.OPENAI_BASE_URL
)

order_stats = {'valid': 0, 'invalid': 0}
order_category_stats = {}
order_number_stats = {}
kimi_analysis_result = {}

socketio = None

def start_kafka_consumer():
    global socketio
    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC,
        bootstrap_servers=[Config.KAFKA_BROKER],
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Kafka 消费者启动中...")

    for message in consumer:
        data = message.value
        print(f"接到 Kafka 消息: {data}")

        update_order_stats(data.get('order_stats', {}))
        update_order_category_stats(data.get('order_category_stats', []))
        update_order_number_stats(data.get('order_number_stats', []))

        emit_data_to_frontend()

        send_to_kimi_api(data)

def update_order_stats(new_stats):
    order_stats['valid'] += new_stats.get('valid', 0)
    order_stats['invalid'] += new_stats.get('invalid', 0)

def update_order_category_stats(new_category_stats):
    for category_stats in new_category_stats:
        for category, stats in category_stats.items():
            if category not in order_category_stats:
                order_category_stats[category] = {'Y': 0, 'N': 0, 'total_quantity': 0}
            order_category_stats[category]['Y'] += stats.get('Y', 0)
            order_category_stats[category]['N'] += stats.get('N', 0)
            order_category_stats[category]['total_quantity'] += stats.get('total_quantity', 0)

def update_order_number_stats(new_number_stats):
    for number_stats in new_number_stats:
        for order_name, stats in number_stats.items():
            if order_name not in order_number_stats:
                order_number_stats[order_name] = {'Y': 0, 'N': 0}
            order_number_stats[order_name]['Y'] += stats.get('Y', 0)
            order_number_stats[order_name]['N'] += stats.get('N', 0)

def emit_data_to_frontend():
    global socketio
    socketio.emit('update_data', {
        'order_stats': order_stats,
        'order_category_stats': order_category_stats,
        'order_number_stats': order_number_stats
    })
    print("发送到前端的数据：", {
        'order_stats': order_stats,
        'order_category_stats': order_category_stats,
        'order_number_stats': order_number_stats
    })

def send_to_kimi_api(data):
    try:
        completion = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system", "content": "你是Kimi，由Moonshot AI提供的智能助手,下面我会每隔一段时间给你发送一串数据，请对这些数据中的订单类别进行实时分析根据每一次发送的数据找出最优生产建议..."},
                {"role": "user", "content": json.dumps(data)}
            ],
            temperature=0.3,
        )
        ai_response = completion.choices[0].message.content
        global kimi_analysis_result
        kimi_analysis_result = ai_response
        print("Kimi 分析结果：", ai_response)

        socketio.emit('update_data', {
            'kimi_analysis_result': ai_response
        })
    except Exception as e:
        print("Kimi API 请求异常：", str(e))