# 导入必要的模块
import os
from kafka import KafkaProducer
import time
import random
from datetime import datetime
import json
import requests  # 新增请求库

def generate_random_data():
    """
    随机生成订单数据，用于模拟订单系统的数据流。
    返回的数据包括订单分类、订单编号、订单数量、订单日期和订单有效性。
    """
    order_categories = ['Electronics', 'Clothing', 'Books', 'Furniture', 'Toys']
    order_names = ['1001', '1002', '1003', '1004', '1005']
    order_quantity = random.randint(1, 50)
    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_valid = 'Y' if random.random() > 0.5 else 'N'

    order_category = random.choice(order_categories)
    order_name = random.choice(order_names)

    return {
        'order_category': order_category,
        'order_name': order_name,
        'order_quantity': order_quantity,
        'order_date': order_date,
        'is_valid': is_valid
    }
def generate_random_user_data():
    """
    随机生成用户订单数据，用于模拟用户订单系统的数据流。
    返回的数据包括订单分类、订单编号、订单数量、订单日期、订单有效性、顾客ID和顾客评分。
    """
    data = generate_random_data()  # 先生成基础订单数据
    user_id = random.randint(1, 100)  # 新增顾客ID
    rating = random.choice([0, 50, 100])  # 新增顾客评分（0:不喜欢，50:还行，100:喜欢）

    data.update({
        'user_id': user_id,
        'rating': rating
    })

    return data

def analyze_with_kimi(data):
    """向Kimi发送数据分析请求"""
    try:
        response = requests.post(
            'kimi-free-api',  # 替换实际API地址
            headers={'Authorization': f'Bearer {os.environ["MOONSHOT_API_KEY"]}'},  # 添加认证信息
            json={'order_data': data},
            timeout=20
        )
        if response.status_code == 200:
            print(f"[Kimi分析] {response.json().get('analysis', '')}")
        else:
            print(f"[分析失败] 状态码：{response.status_code}")
    except Exception as e:
        print(f"[分析异常] {str(e)}")

# 生产者配置
producer = KafkaProducer(
    bootstrap_servers=[os.environ['KAFKA_BROKER']],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    while True:
        # 生成原始订单数据（无顾客ID和评分）
        order_data = generate_random_data()
        print('原始订单数据为:', order_data)

        # 发送到原始订单主题
        producer.send('orders', value=order_data)
        producer.flush()

        # 生成用户订单数据（包含顾客ID和评分）
        user_data = generate_random_user_data()
        print('用户订单数据为:', user_data)

        # 调用Kimi分析
        analyze_with_kimi(user_data)

        # 发送到用户主题
        producer.send('users', value=user_data)
        producer.flush()

        time.sleep(5)
except KeyboardInterrupt:
    producer.close()