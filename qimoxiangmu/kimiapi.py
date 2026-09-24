from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from openai import OpenAI
import os
import logging

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # 生产环境建议指定具体

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KimiAPI')

# 从环境变量获取API密钥
client = OpenAI(
    api_key=os.environ["MOONSHOT_API_KEY"],
    base_url="https://api.moonshot.cn/v1"
)
@app.route('/chat', methods=['GET','POST'])
def chat():
    # 添加请求验证
    if not request.is_json:
        return jsonify({'error': 'Unsupported Media Type'}), 415

    data = request.json
    user_question = data.get('question', '')

    # 更严格的参数校验
    if not isinstance(user_question, str) or len(user_question.strip()) < 2:
        return jsonify({'error': '无效的问题内容'}), 404


    try:
        completion = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system", "content": "你是Kimi，由Moonshot AI提供的智能助手..."},
                {"role": "user", "content": user_question}
            ],
            temperature=0.3,
        )
        ai_response = completion.choices[0].message.content
        # 添加安全响应头
        response = jsonify({'answer': ai_response})
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response
    except Exception as e:
        logger.error(f"API调用失败: {str(e)}")  # 记录错误日志
        return jsonify({'error': '内部服务错误'}), 500
        return render_template('page2.html')
if __name__ == '__main__':
    # 通过环境变量配置
    app.run(host='0.0.0.0',
            port=int(os.getenv("FLASK_PORT", 5000)),
            debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
            threaded=True)