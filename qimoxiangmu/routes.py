from flask import render_template, redirect, url_for, session, jsonify, request
from werkzeug.security import generate_password_hash, check_password_hash
from config import Config
import mysql.connector
from mysql.connector import Error
from services import update_order_stats, update_order_category_stats, update_order_number_stats, emit_data_to_frontend, send_to_kimi_api

order_stats = {'valid': 0, 'invalid': 0}
order_category_stats = {}
order_number_stats = {}
kimi_analysis_result = {}

def favicon():
    return app.send_static_file('favicon.ico')

def home():
    return render_template('login.html')

def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')

    hashed_password = generate_password_hash(password)

    try:
        conn = mysql.connector.connect(**Config.DB_CONFIG)
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

def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    try:
        conn = mysql.connector.connect(**Config.DB_CONFIG)
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

def dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('dashboard.html')

def page1():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('kimi_analysis.html')

def bytime():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('bytime.html')

def kimi():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('kimi.html')

def chat():
    if not request.is_json:
        return jsonify({'error': 'Unsupported Media Type'}), 415

    data = request.json
    user_question = data.get('question', '')

    if not isinstance(user_question, str) or len(user_question.strip()) < 2:
        return jsonify({'error': '无效的问题内容'}), 404

    try:
        completion = client2.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[
                {"role": "system", "content": "你是AI，由Moonshot AI提供的智能助手我将询问你一些关于订单方面的问题..."},
                {"role": "user", "content": user_question}
            ],
            temperature=0.3,
        )
        ai_response = completion.choices[0].message.content
        response = jsonify({'answer': ai_response})
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response
    except Exception as e:
        return jsonify({'error': '内部服务错误'}), 500

def index():
    if not session.get('logged_in'):
        return redirect(url_for('home'))
    return render_template('show.html')

def logout():
    session.pop('logged_in', None)
    session.pop('username', None)
    return redirect(url_for('home'))

def kimi_analysis():
    return render_template('kimi_analysis.html')

def get_kimi_analysis():
    return jsonify(kimi_analysis_result)