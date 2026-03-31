"""
🏭 نظام إدارة الصيانة الموحد - ملف واحد متكامل v3.0
GUI + Bot + Flask Analytics + Database + Logging
"""

import sys
import os
import pandas as pd
import logging
import threading
import time
import subprocess
import hashlib
import json
import random
import webbrowser
from datetime import datetime, timedelta
from collections import defaultdict

# PyQt6
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QMessageBox, QTabWidget, QFormLayout, QGroupBox,
    QCompleter, QScrollArea, QFrame, QSpinBox, QDoubleSpinBox,
    QTextEdit, QStatusBar, QComboBox, QTimeEdit, QDialog,
    QCheckBox, QDateEdit, QListWidget, QListWidgetItem
)
from PyQt6.QtCore import Qt, QTimer, QTime, QDate, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QTextCursor

# Telegram Bot
import telebot
from telebot import types
from dotenv import load_dotenv

# Flask for Analytics
from flask import Flask, render_template_string
import plotly.graph_objects as go
import plotly.express as px

# ========================== CONFIGURATION ==========================
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN', '8639931680:AAEbrBQDWb40m3rAGjvlHicmnrG6iDGYxGo')
DATA_DIR = 'maintenance_data'
LOG_FILE = 'maintenance_system.log'

os.makedirs(DATA_DIR, exist_ok=True)

# ملفات البيانات
CONFIG_FILE = f'{DATA_DIR}/config.csv'
READINGS_FILE = f'{DATA_DIR}/readings.csv'
USERS_FILE = f'{DATA_DIR}/users.csv'
EMPLOYEES_FILE = f'{DATA_DIR}/employees.csv'
TASKS_FILE = f'{DATA_DIR}/tasks.csv'
REQUESTS_FILE = f'{DATA_DIR}/requests.csv'

# ========================== LOGGING ==========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========================== DATABASE INITIALIZATION ==========================
def init_database():
    """تهيئة قاعدة البيانات"""
    try:
        # Config
        if not os.path.exists(CONFIG_FILE):
            pd.DataFrame(columns=['Factory', 'Hall', 'Line', 'Machine', 'Task', 'Normal_Val', 'Daily_Required']).to_csv(
                CONFIG_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Config")

        # Users
        if not os.path.exists(USERS_FILE):
            pd.DataFrame(columns=['Username', 'Password_Hash', 'Role', 'Created_At']).to_csv(
                USERS_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Users")
            default_user = pd.DataFrame([[
                'admin',
                hashlib.sha256('admin123'.encode()).hexdigest(),
                'admin',
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ]], columns=['Username', 'Password_Hash', 'Role', 'Created_At'])
            default_user.to_csv(USERS_FILE, index=False, encoding='utf-8-sig')

        # Employees
        if not os.path.exists(EMPLOYEES_FILE):
            pd.DataFrame(columns=['Chat_ID', 'Employee_Name', 'Shift', 'Status', 'Registered_At']).to_csv(
                EMPLOYEES_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Employees")

        # Readings
        if not os.path.exists(READINGS_FILE):
            pd.DataFrame(columns=[
                'Timestamp', 'Employee', 'Shift', 'Factory', 'Hall', 'Line', 
                'Machine', 'Task', 'Value', 'Normal_Val', 'Deviation', 'Image_Path', 'Verified'
            ]).to_csv(READINGS_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Readings")

        # Tasks
        if not os.path.exists(TASKS_FILE):
            pd.DataFrame(columns=[
                'Task_ID', 'Factory', 'Hall', 'Line', 'Machine', 
                'Shift', 'Created_At', 'Due_Time', 'Status', 'Assigned_To', 'Completed_At'
            ]).to_csv(TASKS_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Tasks")

        # Requests
        if not os.path.exists(REQUESTS_FILE):
            pd.DataFrame(columns=[
                'Request_ID', 'Factory', 'Hall', 'Line', 'Machine',
                'Frequency_Minutes', 'Last_Sent', 'Status'
            ]).to_csv(REQUESTS_FILE, index=False, encoding='utf-8-sig')
            logger.info("✅ تم إنشاء ملف Requests")

    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة قاعدة البيانات: {e}", exc_info=True)

# ========================== UTILITIES ==========================
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def safe_read_csv(filepath):
    try:
        if os.path.exists(filepath):
            return pd.read_csv(filepath)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ خطأ في قراءة {filepath}: {e}")
        return pd.DataFrame()

def safe_save_csv(df, filepath):
    try:
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"✅ تم حفظ {os.path.basename(filepath)}")
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ {filepath}: {e}")

# ========================== TELEGRAM BOT ==========================
bot = telebot.TeleBot(API_TOKEN, parse_mode="HTML")
user_steps = {}
stop_requests = False

def log_bot_action(chat_id, action, details=""):
    msg = f"🤖 [{action}] Chat: {chat_id}"
    if details:
        msg += f" | {details}"
    logger.info(msg)

@bot.message_handler(commands=['start'])
def start_bot(message):
    chat_id = message.chat.id
    user_name = message.from_user.first_name or "موظف"
    
    log_bot_action(chat_id, "START", f"الاسم: {user_name}")
    
    df_emp = safe_read_csv(EMPLOYEES_FILE)
    if chat_id not in df_emp['Chat_ID'].values:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("الوردية الأولى ☀️", callback_data="shift_1"))
        markup.add(types.InlineKeyboardButton("الوردية الثانية 🌙", callback_data="shift_2"))
        
        bot.send_message(chat_id, f"👋 مرحباً {user_name}!\n\nاختر ورديتك:", reply_markup=markup)
        log_bot_action(chat_id, "NEW_USER", "بانتظار اختيار الوردية")
    else:
        show_main_menu(message)

@bot.callback_query_handler(func=lambda call: call.data.startswith('shift_'))
def handle_shift_selection(call):
    chat_id = call.message.chat.id
    shift = "الأولى" if call.data == "shift_1" else "الثانية"
    
    df_emp = safe_read_csv(EMPLOYEES_FILE)
    new_emp = pd.DataFrame([[
        chat_id,
        call.from_user.first_name or "موظف",
        shift,
        "نشط",
        datetime.now().strftime("%Y-%m-%d %H:%M")
    ]], columns=['Chat_ID', 'Employee_Name', 'Shift', 'Status', 'Registered_At'])
    
    df_emp = pd.concat([df_emp, new_emp], ignore_index=True)
    safe_save_csv(df_emp, EMPLOYEES_FILE)
    
    log_bot_action(chat_id, "SHIFT_SELECT", f"الوردية: {shift}")
    
    bot.edit_message_text(
        f"✅ تم تسجيلك في الوردية {shift}!\n\n🚀 ابدأ الآن:",
        chat_id, call.message.message_id
    )
    
    show_main_menu(call.message)

def show_main_menu(message):
    chat_id = message.chat.id
    df_emp = safe_read_csv(EMPLOYEES_FILE)
    emp = df_emp[df_emp['Chat_ID'] == chat_id]
    
    if emp.empty:
        return
    
    shift = emp.iloc[0]['Shift']
    missing = get_missing_tasks_for_shift(shift)
    
    markup = types.InlineKeyboardMarkup()
    if missing:
        markup.add(types.InlineKeyboardButton(f"🚨 تسجيل النواقص ({len(missing)})", callback_data="missing_list"))
    markup.add(types.InlineKeyboardButton("🏭 تصفح المعدات", callback_data="browse_machines"))
    markup.add(types.InlineKeyboardButton("📊 إحصائياتي", callback_data="my_stats"))
    
    msg = f"👤 مرحباً {emp.iloc[0]['Employee_Name']}\n"
    msg += f"⏰ الوردية: {shift}\n\n"
    if missing:
        msg += f"⚠️ <b>{len(missing)} قراءات مطلوبة اليوم</b>"
    else:
        msg += "✅ جميع القراءات مكتملة"
    
    bot.send_message(chat_id, msg, reply_markup=markup)
    log_bot_action(chat_id, "SHOW_MENU", f"النواقص: {len(missing)}")

def get_missing_tasks_for_shift(shift):
    try:
        df_config = safe_read_csv(CONFIG_FILE)
        df_readings = safe_read_csv(READINGS_FILE)
        
        if df_config.empty:
            return []
        
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        
        if not df_readings.empty:
            today_readings = df_readings[
                (df_readings['Timestamp'].str.startswith(today)) &
                (df_readings['Shift'] == shift)
            ]
        else:
            today_readings = pd.DataFrame()
        
        missing = []
        for _, config_row in df_config.iterrows():
            required = int(config_row.get('Daily_Required', 1))
            machine = str(config_row['Machine'])
            task = str(config_row['Task']).strip() if pd.notna(config_row['Task']) else ""
            
            count = len(today_readings[
                (today_readings['Machine'].astype(str) == machine) &
                (today_readings['Task'].fillna("").astype(str).str.strip() == task)
            ])
            
            if count < required:
                missing.append(config_row.to_dict())
        
        return missing
    except Exception as e:
        logger.error(f"❌ خطأ في get_missing_tasks: {e}")
        return []

@bot.message_handler(func=lambda m: True)
def handle_reading_input(message):
    chat_id = message.chat.id
    
    if chat_id not in user_steps or 't' not in user_steps[chat_id]:
        return
    
    try:
        value = float(message.text)
        info = user_steps[chat_id]
        
        df_config = safe_read_csv(CONFIG_FILE)
        task_str = str(info.get('t', '')).strip()
        
        matching = df_config[
            (df_config['Factory'].astype(str) == str(info['f'])) &
            (df_config['Hall'].astype(str) == str(info['h'])) &
            (df_config['Line'].astype(str) == str(info['l'])) &
            (df_config['Machine'].astype(str) == str(info['m'])) &
            (df_config['Task'].fillna("").astype(str).str.strip() == task_str)
        ]
        
        if matching.empty:
            bot.reply_to(message, "⚠️ لم يتم العثور على بيانات المعدة")
            logger.warning(f"⚠️ بيانات مفقودة: {info['m']}")
            return
        
        normal = float(matching['Normal_Val'].iloc[0])
        deviation = abs(value - normal)
        
        # طلب صورة عشوائياً
        if random.random() < 0.3:
            user_steps[chat_id]['temp_value'] = value
            user_steps[chat_id]['temp_normal'] = normal
            user_steps[chat_id]['temp_deviation'] = deviation
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("📸 أرسل صورة للتحقق", callback_data="upload_image"))
            markup.add(types.InlineKeyboardButton("⏭️ تخطي", callback_data="skip_image"))
            
            bot.send_message(chat_id, 
                "🔍 للتحقق من دقة القراءة، أرسل صورة للعداد أو تعاريفه:\n\n"
                "(أو اختر تخطي للمتابعة)", 
                reply_markup=markup)
            log_bot_action(chat_id, "IMAGE_REQUEST", f"جهاز: {info['m']}")
            return
        
        save_reading(chat_id, info, value, normal, deviation, None)
        
    except ValueError:
        bot.reply_to(message, "⚠️ يرجى إدخال رقم صحيح")
        logger.warning(f"⚠️ إدخال غير صحيح من {chat_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة الرسالة: {e}", exc_info=True)
        bot.reply_to(message, f"⚠️ خطأ: {str(e)}")

def save_reading(chat_id, info, value, normal, deviation, image_path):
    try:
        df_emp = safe_read_csv(EMPLOYEES_FILE)
        emp = df_emp[df_emp['Chat_ID'] == chat_id]
        
        if emp.empty:
            return
        
        emp_name = emp.iloc[0]['Employee_Name']
        shift = emp.iloc[0]['Shift']
        
        df_readings = safe_read_csv(READINGS_FILE)
        new_reading = pd.DataFrame([[
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            emp_name,
            shift,
            info['f'], info['h'], info['l'], info['m'],
            info.get('t', ''),
            value, normal, deviation,
            image_path or "",
            "نعم" if image_path else "لا"
        ]], columns=df_readings.columns)
        
        df_readings = pd.concat([df_readings, new_reading], ignore_index=True)
        safe_save_csv(df_readings, READINGS_FILE)
        
        task_name = info.get('t') or "إجمالي"
        status = "✅" if deviation <= normal * 0.1 else "⚠️"
        
        bot.send_message(chat_id, 
            f"{status} <b>تم تسجيل القراءة</b>\n\n"
            f"📊 {task_name}: <b>{value} A</b>\n"
            f"⚡ الطبيعي: <b>{normal} A</b>\n"
            f"📈 الحيود: <b>{deviation:.2f} A</b>")
        
        log_bot_action(chat_id, "READING_SAVED", f"{info['m']}: {value}A | حيود: {deviation:.2f}")
        
        queue = info.get('queue', [])
        if queue:
            next_task = queue.pop(0)
            user_steps[chat_id] = {
                'f': info['f'], 'h': info['h'], 'l': info['l'], 'm': info['m'],
                't': next_task, 'queue': queue
            }
            next_label = next_task or "إجمالي"
            bot.send_message(chat_id, f"⏭️ التالي: أدخل قراءة <b>({next_label})</b>:")
        else:
            del user_steps[chat_id]
            bot.send_message(chat_id, "🎉 ممتاز! تم إكمال جميع المهام")
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ القراءة: {e}", exc_info=True)

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    chat_id = call.message.chat.id
    data = call.data
    
    try:
        bot.answer_callback_query(call.id)
    except:
        pass
    
    if data == "upload_image":
        bot.send_message(chat_id, "📸 أرسل صورة الآن:")
        log_bot_action(chat_id, "IMAGE_UPLOAD", "بانتظار صورة")
    elif data == "skip_image":
        info = user_steps.get(chat_id, {})
        save_reading(chat_id, info, 
                    info.get('temp_value'), 
                    info.get('temp_normal'),
                    info.get('temp_deviation'),
                    None)
    elif data == "missing_list":
        df_emp = safe_read_csv(EMPLOYEES_FILE)
        emp = df_emp[df_emp['Chat_ID'] == chat_id]
        if not emp.empty:
            shift = emp.iloc[0]['Shift']
            missing = get_missing_tasks_for_shift(shift)
            
            if not missing:
                bot.edit_message_text("✅ جميع القراءات مكتملة!", chat_id, call.message.message_id)
                return
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            machines = {}
            
            for row in missing:
                key = (str(row['Factory']), str(row['Hall']), str(row['Line']), str(row['Machine']))
                if key not in machines:
                    machines[key] = {'count': 1}
                else:
                    machines[key]['count'] += 1
            
            for i, (key, info) in enumerate(list(machines.items())[:10]):
                if i >= 10:
                    break
                f, h, l, m = key
                label = f"🛤️ {l} → {m[:12]} ({info['count']})"
                markup.add(types.InlineKeyboardButton(label, callback_data=f"record|{m}"))
            
            bot.edit_message_text(f"⚠️ <b>اختر المعدة ({len(machines)} متاح)</b>:", 
                                  chat_id, call.message.message_id, reply_markup=markup)
    
    elif data.startswith("record|"):
        machine_name = data.split("|")[1]
        df_config = safe_read_csv(CONFIG_FILE)
        machine_data = df_config[df_config['Machine'].astype(str) == machine_name]
        
        if not machine_data.empty:
            first_row = machine_data.iloc[0]
            tasks = machine_data['Task'].tolist()
            
            user_steps[chat_id] = {
                'f': str(first_row['Factory']),
                'h': str(first_row['Hall']),
                'l': str(first_row['Line']),
                'm': machine_name,
                't': str(tasks[0]).strip() if tasks else "",
                'queue': [str(t).strip() for t in tasks[1:] if pd.notna(t)]
            }
            
            task_label = str(tasks[0]).strip() if tasks and pd.notna(tasks[0]) else "إجمالي"
            bot.send_message(chat_id, f"📝 أدخل قراءة <b>({task_label})</b> لـ <b>{machine_name}</b>:")
            log_bot_action(chat_id, "RECORD_START", f"جهاز: {machine_name}")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    chat_id = message.chat.id
    
    try:
        if chat_id not in user_steps or 'temp_value' not in user_steps[chat_id]:
            bot.reply_to(message, "❌ لم يكن هناك طلب صورة حالياً")
            return
        
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_dir = f"{DATA_DIR}/images"
        os.makedirs(image_dir, exist_ok=True)
        image_path = f"{image_dir}/{chat_id}_{timestamp}.jpg"
        
        with open(image_path, 'wb') as f:
            f.write(downloaded_file)
        
        logger.info(f"📸 تم حفظ صورة: {image_path}")
        
        info = user_steps[chat_id]
        save_reading(chat_id, info,
                    info.get('temp_value'),
                    info.get('temp_normal'),
                    info.get('temp_deviation'),
                    image_path)
        
        bot.reply_to(message, "✅ تم حفظ الصورة والقراءة")
        
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة الصورة: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ: {str(e)}")

def start_periodic_reminders():
    global stop_requests
    
    while not stop_requests:
        try:
            df_emp = safe_read_csv(EMPLOYEES_FILE)
            if not df_emp.empty:
                for shift in ["الأولى", "الثانية"]:
                    missing = get_missing_tasks_for_shift(shift)
                    if missing:
                        for _, emp in df_emp[df_emp['Shift'] == shift].iterrows():
                            try:
                                bot.send_message(emp['Chat_ID'],
                                    f"🔔 <b>تذكير دوري</b> ⏰\n\n"
                                    f"يوجد {len(missing)} قراءات مطلوبة\n\n"
                                    f"استخدم /start للبدء")
                                log_bot_action(emp['Chat_ID'], "REMINDER", f"النواقص: {len(missing)}")
                            except:
                                pass
            
            time.sleep(900)  # 15 دقيقة
        except Exception as e:
            logger.error(f"❌ خطأ في الطلبات الدورية: {e}")
            time.sleep(60)

# ========================== FLASK ANALYTICS ==========================
app = Flask(__name__)

@app.route('/')
def analytics_dashboard():
    try:
        df_readings = safe_read_csv(READINGS_FILE)
        
        if df_readings.empty:
            return "<h1>❌ لا توجد بيانات حتى الآن</h1>"
        
        # تحويل الأعمدة الرقمية
        df_readings['Value'] = pd.to_numeric(df_readings['Value'], errors='coerce')
        df_readings['Normal_Val'] = pd.to_numeric(df_readings['Normal_Val'], errors='coerce')
        df_readings['Deviation'] = pd.to_numeric(df_readings['Deviation'], errors='coerce')
        
        # إزالة الصفوف الفارغة
        df_readings = df_readings.dropna(subset=['Value', 'Normal_Val', 'Deviation'])
        
        # 1. رسم بياني للقراءات
        fig1 = px.line(
            df_readings.tail(100),
            x='Timestamp',
            y='Value',
            color='Machine',
            title='📊 تطور القراءات عبر الوقت',
            labels={'Value': 'الأمبير (A)', 'Timestamp': 'الوقت', 'Machine': 'المعدة'},
            height=500
        )
        fig1.update_layout(
            hovermode='x unified',
            template='plotly_dark',
            font=dict(family="Arial, sans-serif", size=12)
        )
        
        # 2. رسم بياني الحيود (عمودي)
        df_deviation = df_readings.tail(50).copy()
        fig2 = px.bar(
            df_deviation,
            x='Machine',
            y='Deviation',
            color='Deviation',
            title='📈 الحيود عن القيمة الطبيعية (آخر 50 قراءة)',
            labels={'Deviation': 'الحيود (A)', 'Machine': 'المعدة'},
            color_continuous_scale='Reds',
            height=500
        )
        fig2.update_layout(
            template='plotly_dark',
            font=dict(family="Arial, sans-serif", size=12),
            xaxis_tickangle=-45
        )
        
        # 3. توزيع الموظفين
        readings_by_emp = df_readings.groupby('Employee').size().reset_index(name='عدد القراءات')
        fig3 = px.pie(
            readings_by_emp,
            names='Employee',
            values='عدد القراءات',
            title='👥 توزيع القراءات بين الموظفين',
            height=500
        )
        fig3.update_layout(
            template='plotly_dark',
            font=dict(family="Arial, sans-serif", size=12)
        )
        
        # 4. رسم توزيع القراءات حسب الوردية
        shift_data = df_readings.groupby('Shift').size().reset_index(name='عدد القراءات')
        fig4 = px.bar(
            shift_data,
            x='Shift',
            y='عدد القراءات',
            color='Shift',
            title='⏰ توزيع القراءات حسب الوردية',
            labels={'Shift': 'الوردية', 'عدد القراءات': 'العدد'},
            color_discrete_map={'الأولى': '#FFD700', 'الثانية': '#4B0082'},
            height=400
        )
        fig4.update_layout(template='plotly_dark', font=dict(family="Arial, sans-serif", size=12))
        
        # 5. جدول الحيود العالية
        high_deviation = df_readings[df_readings['Deviation'] > df_readings['Normal_Val'] * 0.15].copy()
        
        # إحصائيات
        total_readings = len(df_readings)
        avg_deviation = df_readings['Deviation'].mean()
        high_dev_count = len(high_deviation)
        verified = len(df_readings[df_readings['Verified'] == 'نعم'])
        
        html = f"""
        <!DOCTYPE html>
        <html dir="rtl" lang="ar">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>📊 لوحة تحليل البيانات - نظام إدارة الصيانة</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: 'Segoe UI', 'Arial', sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 20px;
                    min-height: 100vh;
                }}
                .container {{
                    max-width: 1600px;
                    margin: 0 auto;
                }}
                .header {{
                    background: white;
                    padding: 30px;
                    border-radius: 15px;
                    margin-bottom: 30px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    text-align: center;
                }}
                .header h1 {{
                    color: #333;
                    font-size: 2.5em;
                    margin-bottom: 10px;
                }}
                .header p {{
                    color: #666;
                    font-size: 1.1em;
                }}
                .stats {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-card {{
                    background: white;
                    padding: 25px;
                    border-radius: 12px;
                    text-align: center;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                    border-top: 4px solid #667eea;
                    transition: transform 0.3s, box-shadow 0.3s;
                }}
                .stat-card:hover {{
                    transform: translateY(-5px);
                    box-shadow: 0 10px 25px rgba(0,0,0,0.15);
                }}
                .stat-card h3 {{
                    color: #666;
                    font-size: 0.95em;
                    margin-bottom: 15px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }}
                .stat-card .value {{
                    font-size: 2.5em;
                    font-weight: bold;
                    color: #667eea;
                }}
                .stat-card.warning .value {{
                    color: #f39c12;
                }}
                .stat-card.danger .value {{
                    color: #e74c3c;
                }}
                .stat-card.success .value {{
                    color: #27ae60;
                }}
                .chart {{
                    background: white;
                    padding: 25px;
                    border-radius: 12px;
                    margin-bottom: 30px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                }}
                .chart h3 {{
                    color: #333;
                    margin-bottom: 20px;
                    font-size: 1.3em;
                    border-bottom: 2px solid #667eea;
                    padding-bottom: 10px;
                }}
                .chart-row {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .table-container {{
                    background: white;
                    padding: 25px;
                    border-radius: 12px;
                    margin-bottom: 30px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                    overflow-x: auto;
                }}
                .table-container h3 {{
                    color: #333;
                    margin-bottom: 20px;
                    font-size: 1.3em;
                    border-bottom: 2px solid #e74c3c;
                    padding-bottom: 10px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th {{
                    background-color: #34495e;
                    color: white;
                    padding: 15px;
                    text-align: right;
                    font-weight: bold;
                }}
                td {{
                    padding: 12px 15px;
                    border-bottom: 1px solid #ecf0f1;
                }}
                tr:hover {{
                    background-color: #f8f9fa;
                }}
                tr.high-deviation {{
                    background-color: #ffebee;
                }}
                .footer {{
                    text-align: center;
                    color: white;
                    padding: 20px;
                    margin-top: 30px;
                }}
                @media (max-width: 768px) {{
                    .stats {{
                        grid-template-columns: 1fr;
                    }}
                    .chart-row {{
                        grid-template-columns: 1fr;
                    }}
                    .header h1 {{
                        font-size: 1.8em;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 لوحة تحليل البيانات</h1>
                    <p>نظام إدارة الصيانة - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="stats">
                    <div class="stat-card success">
                        <h3>📋 إجمالي القراءات</h3>
                        <div class="value">{total_readings}</div>
                    </div>
                    <div class="stat-card">
                        <h3>📊 متوسط الحيود</h3>
                        <div class="value">{avg_deviation:.2f}A</div>
                    </div>
                    <div class="stat-card danger">
                        <h3>⚠️ قراءات شاذة</h3>
                        <div class="value">{high_dev_count}</div>
                    </div>
                    <div class="stat-card warning">
                        <h3>✅ قراءات موثقة</h3>
                        <div class="value">{verified}</div>
                    </div>
                </div>
                
                <div class="chart-row">
                    <div class="chart">
                        <h3>📈 تطور القراءات</h3>
                        {fig1.to_html(include_plotlyjs=False, div_id="chart1")}
                    </div>
                </div>
                
                <div class="chart-row">
                    <div class="chart">
                        <h3>📊 الحيود عن القيمة الطبيعية</h3>
                        {fig2.to_html(include_plotlyjs=False, div_id="chart2")}
                    </div>
                </div>
                
                <div class="chart-row">
                    <div class="chart">
                        <h3>👥 توزيع الموظفين</h3>
                        {fig3.to_html(include_plotlyjs=False, div_id="chart3")}
                    </div>
                    <div class="chart">
                        <h3>⏰ توزيع الورديات</h3>
                        {fig4.to_html(include_plotlyjs=False, div_id="chart4")}
                    </div>
                </div>
                
                {"<div class='table-container'><h3>⚠️ القراءات الشاذة (الحيود > 15%)</h3><table><thead><tr><th>الوقت</th><th>الموظف</th><th>المعدة</th><th>المهمة</th><th>القراءة</th><th>الطبيعي</th><th>الحيود</th></tr></thead><tbody>" + "".join([f"<tr class='high-deviation'><td>{row['Timestamp']}</td><td>{row['Employee']}</td><td>{row['Machine']}</td><td>{row['Task']}</td><td>{row['Value']:.2f}A</td><td>{row['Normal_Val']:.2f}A</td><td><strong>{row['Deviation']:.2f}A</strong></td></tr>" for _, row in high_deviation.iterrows()]) + "</tbody></table></div>" if not high_deviation.empty else ""}
                
                <div class="footer">
                    <p>🏭 نظام إدارة الصيانة الموحد v3.0</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
        
    except Exception as e:
        logger.error(f"❌ خطأ في عرض التحليلات: {e}", exc_info=True)
        return f"<h1>❌ خطأ: {e}</h1>"

# ========================== PyQt6 GUI ==========================
class PartRow:
    def __init__(self, layout, name_input, ampere_input, frequency_input):
        self.layout = layout
        self.name = name_input
        self.ampere = ampere_input
        self.frequency = frequency_input

class LoginDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🔐 تسجيل الدخول")
        self.setGeometry(200, 200, 450, 250)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #ecf0f1; }
            QLineEdit { 
                border: 2px solid #bdc3c7; 
                border-radius: 8px; 
                padding: 10px; 
                background-color: white;
                font-size: 12px;
            }
            QLineEdit:focus { 
                border: 2px solid #3498db; 
                background-color: #ecf8ff;
            }
            QPushButton {
                border-radius: 8px;
                padding: 10px;
                font-weight: bold;
                border: none;
                font-size: 12px;
            }
            QPushButton#loginBtn {
                background-color: #3498db;
                color: white;
            }
            QPushButton#loginBtn:hover {
                background-color: #2980b9;
            }
            QPushButton#registerBtn {
                background-color: #2ecc71;
                color: white;
            }
            QPushButton#registerBtn:hover {
                background-color: #27ae60;
            }
        """)
        
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        title = QLabel("🏭 نظام إدارة الصيانة")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #2c3e50;")
        layout.addWidget(title)
        
        # اسم المستخدم
        user_layout = QHBoxLayout()
        user_label = QLabel("👤 المستخدم:")
        user_label.setFixedWidth(80)
        self.username = QLineEdit()
        self.username.setPlaceholderText("admin")
        user_layout.addWidget(user_label)
        user_layout.addWidget(self.username)
        layout.addLayout(user_layout)
        
        # كلمة المرور
        pass_layout = QHBoxLayout()
        pass_label = QLabel("🔑 كلمة المرور:")
        pass_label.setFixedWidth(80)
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        self.password.setPlaceholderText("admin123")
        pass_layout.addWidget(pass_label)
        pass_layout.addWidget(self.password)
        layout.addLayout(pass_layout)
        
        # أزرار
        btn_layout = QHBoxLayout()
        login_btn = QPushButton("✅ دخول")
        login_btn.setObjectName("loginBtn")
        login_btn.clicked.connect(self.accept)
        
        register_btn = QPushButton("📝 مستخدم جديد")
        register_btn.setObjectName("registerBtn")
        register_btn.clicked.connect(self.register_user)
        
        btn_layout.addWidget(login_btn)
        btn_layout.addWidget(register_btn)
        layout.addLayout(btn_layout)
        
        self.setLayout(layout)
        self.result = None
    
    def register_user(self):
        new_user = QDialog(self)
        new_user.setWindowTitle("📝 إضافة مستخدم جديد")
        new_user.setGeometry(250, 250, 450, 300)
        new_user.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        
        layout = QVBoxLayout()
        
        username_input = QLineEdit()
        username_input.setPlaceholderText("اسم المستخدم الجديد")
        
        password_input = QLineEdit()
        password_input.setEchoMode(QLineEdit.EchoMode.Password)
        password_input.setPlaceholderText("كلمة المرور")
        
        role_input = QComboBox()
        role_input.addItems(["مشرف", "موظف"])
        
        layout.addWidget(QLabel("👤 اسم المستخدم:"))
        layout.addWidget(username_input)
        layout.addWidget(QLabel("🔑 كلمة المرور:"))
        layout.addWidget(password_input)
        layout.addWidget(QLabel("👨‍💼 الدور:"))
        layout.addWidget(role_input)
        
        btn = QPushButton("✅ إنشاء")
        btn.setStyleSheet("background-color: #2ecc71; color: white; font-weight: bold; padding: 10px; border-radius: 5px;")
        
        def save_user():
            username = username_input.text().strip()
            password = password_input.text().strip()
            role = role_input.currentText()
            
            if not username or not password:
                QMessageBox.warning(new_user, "تنبيه", "يرجى ملء جميع الحقول")
                return
            
            df_users = safe_read_csv(USERS_FILE)
            if username in df_users['Username'].values:
                QMessageBox.warning(new_user, "تنبيه", "المستخدم موجود بالفعل")
                return
            
            new_user_row = pd.DataFrame([[
                username,
                hash_password(password),
                role,
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ]], columns=['Username', 'Password_Hash', 'Role', 'Created_At'])
            
            df_users = pd.concat([df_users, new_user_row], ignore_index=True)
            safe_save_csv(df_users, USERS_FILE)
            
            logger.info(f"✅ تم إضافة مستخدم جديد: {username}")
            QMessageBox.information(new_user, "نجاح", "تم إنشاء المستخدم بنجاح")
            new_user.accept()
        
        btn.clicked.connect(save_user)
        layout.addWidget(btn)
        new_user.setLayout(layout)
        new_user.exec()

class MaintenanceApp(QMainWindow):
    def __init__(self):
        super().__init__()
        logger.info("🚀 بدء تشغيل البرنامج الرئيسي")
        
        init_database()
        
        # تسجيل الدخول
        login = LoginDialog()
        if login.exec() == QDialog.DialogCode.Accepted:
            username = login.username.text() or "admin"
            password = login.password.text() or "admin123"
            
            if self.verify_login(username, password):
                self.current_user = username
                self.init_ui()
                logger.info(f"✅ تسجيل دخول المستخدم: {username}")
            else:
                logger.warning(f"❌ محاولة تسجيل دخول فاشلة: {username}")
                QMessageBox.critical(self, "خطأ", "بيانات الدخول غير صحيحة")
                sys.exit()
        else:
            sys.exit()
    
    def verify_login(self, username, password):
        df_users = safe_read_csv(USERS_FILE)
        user = df_users[df_users['Username'] == username]
        
        if user.empty:
            return False
        
        stored_hash = user.iloc[0]['Password_Hash']
        return stored_hash == hash_password(password)
    
    def init_ui(self):
        self.setWindowTitle("⚡ نظام إدارة الصيانة الموحد v3.0")
        self.setGeometry(30, 30, 1700, 1050)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        
        self.setStyleSheet("""
            QMainWindow { background-color: #ecf0f1; }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #bdc3c7;
                border-radius: 8px;
                margin-top: 10px;
                background-color: white;
                padding: 5px;
                color: #2c3e50;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 15px;
                padding: 0 8px;
            }
            QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
                border: 1px solid #95a5a6;
                border-radius: 5px;
                padding: 8px;
                background-color: white;
                color: #2c3e50;
            }
            QLineEdit:focus {
                border: 2px solid #3498db;
                background-color: #ecf8ff;
            }
            QPushButton {
                border-radius: 5px;
                padding: 10px 15px;
                font-weight: bold;
                border: none;
            }
            QPushButton#primaryBtn {
                background-color: #3498db;
                color: white;
            }
            QPushButton#primaryBtn:hover {
                background-color: #2980b9;
            }
            QPushButton#successBtn {
                background-color: #2ecc71;
                color: white;
            }
            QPushButton#successBtn:hover {
                background-color: #27ae60;
            }
            QPushButton#dangerBtn {
                background-color: #e74c3c;
                color: white;
            }
            QPushButton#dangerBtn:hover {
                background-color: #c0392b;
            }
            QTableWidget {
                background-color: white;
                alternate-background-color: #f8f9fa;
                border: 1px solid #bdc3c7;
                border-radius: 5px;
                gridline-color: #ecf0f1;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                padding: 8px;
                border: none;
                font-weight: bold;
            }
            QTabWidget::pane {
                border: 1px solid #bdc3c7;
            }
            QTabBar::tab {
                background-color: #bdc3c7;
                color: #2c3e50;
                padding: 8px 20px;
                border: 1px solid #95a5a6;
                border-bottom: none;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background-color: #3498db;
                color: white;
            }
            QTextEdit {
                background-color: #1e1e1e;
                color: #00ff00;
                font-family: 'Courier New';
                border: 1px solid #bdc3c7;
                border-radius: 5px;
            }
        """)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # شريط علوي
        top_bar = QFrame()
        top_bar.setStyleSheet("background-color: #34495e; border-radius: 8px;")
        top_layout = QHBoxLayout(top_bar)
        
        title_lbl = QLabel("⚡ نظام إدارة الصيانة الموحد")
        title_lbl.setStyleSheet("color: white; font-size: 18px; font-weight: bold;")
        
        user_lbl = QLabel(f"👤 {self.current_user}")
        user_lbl.setStyleSheet("color: white; font-size: 12px;")
        
        self.bot_status = QLabel("🤖 البوت: 🟡")
        self.bot_status.setStyleSheet("color: yellow; font-size: 12px; font-weight: bold;")
        
        bot_btn = QPushButton("تشغيل البوت")
        bot_btn.setObjectName("primaryBtn")
        bot_btn.setFixedWidth(150)
        bot_btn.clicked.connect(self.toggle_bot)
        
        analytics_btn = QPushButton("📊 الرسوم البيانية")
        analytics_btn.setObjectName("successBtn")
        analytics_btn.setFixedWidth(150)
        analytics_btn.clicked.connect(lambda: webbrowser.open("http://127.0.0.1:5000"))
        
        top_layout.addWidget(title_lbl)
        top_layout.addStretch()
        top_layout.addWidget(user_lbl)
        top_layout.addWidget(self.bot_status)
        top_layout.addWidget(bot_btn)
        top_layout.addWidget(analytics_btn)
        
        main_layout.addWidget(top_bar)
        
        # التبويبات
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        self.setup_machines_tab()
        self.setup_employees_tab()
        self.setup_tasks_tab()
        self.setup_requests_tab()
        self.setup_readings_tab()
        self.setup_logs_tab()
        
        self.statusBar().showMessage("✅ البرنامج جاهز - جميع الأنظمة تعمل بكفاءة", 5000)
        
        self.bot_thread = None
        self.bot_process = None
        
        logger.info("✅ تم تحضير الواجهة الرئيسية")
    
    def setup_machines_tab(self):
        """تبويب إدارة المعدات"""
        tab = QWidget()
        layout = QHLayout(tab)
        
        # الجانب الأيسر
        form_scroll = QScrollArea()
        form_scroll.setWidgetResizable(True)
        form_scroll.setFixedWidth(600)
        
        form_container = QWidget()
        self.form_layout = QVBoxLayout(form_container)
        
        # بيانات أساسية
        basic_group = QGroupBox("🏭 معلومات المعدة الأساسية")
        basic_layout = QFormLayout()
        
        self.factory_input = QLineEdit()
        self.factory_input.setPlaceholderText("مثال: مصنع القاهرة")
        
        self.hall_input = QLineEdit()
        self.hall_input.setPlaceholderText("مثال: صالة A")
        
        self.line_input = QLineEdit()
        self.line_input.setPlaceholderText("مثال: خط 1")
        
        self.machine_input = QLineEdit()
        self.machine_input.setPlaceholderText("مثال: موتور رئيسي")
        
        basic_layout.addRow("🏢 المصنع:", self.factory_input)
        basic_layout.addRow("🏛️ الصالة:", self.hall_input)
        basic_layout.addRow("🛤️ الخط:", self.line_input)
        basic_layout.addRow("⚙️ المعدة:", self.machine_input)
        
        basic_group.setLayout(basic_layout)
        self.form_layout.addWidget(basic_group)
        
        # أجزاء المعدة - محسّنة
        parts_group = QGroupBox("📋 أجزاء المعدة والأمبير الطبيعي")
        parts_layout = QVBoxLayout()
        
        # رأس الجدول بألوان
        header_layout = QHBoxLayout()
        header_layout.setSpacing(10)
        
        header_items = [
            ("📝 اسم الجزء", 2, "#3498db"),
            ("⚡ الأمبير", 1, "#e74c3c"),
            ("📊 قراءات/يوم", 1, "#2ecc71"),
            ("", 0, "#95a5a6")
        ]
        
        for text, stretch, color in header_items:
            lbl = QLabel(text)
            lbl.setStyleSheet(f"background-color: {color}; color: white; padding: 8px; border-radius: 4px; font-weight: bold;")
            if stretch > 0:
                header_layout.addWidget(lbl, stretch)
            else:
                header_layout.addWidget(lbl, 0)
        
        parts_layout.addLayout(header_layout)
        
        # منطقة التمرير
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: 2px solid #3498db; border-radius: 5px; background-color: #f8f9fa;")
        
        scroll_widget = QWidget()
        self.parts_form_layout = QVBoxLayout(scroll_widget)
        self.parts_form_layout.setSpacing(10)
        
        self.parts_rows = []
        self.add_part_row_safe()
        
        self.parts_form_layout.addStretch()
        scroll.setWidget(scroll_widget)
        parts_layout.addWidget(scroll)
        
        # أزرار
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        add_btn = QPushButton("➕ إضافة جزء")
        add_btn.setObjectName("primaryBtn")
        add_btn.clicked.connect(self.add_part_row_safe)
        
        save_btn = QPushButton("💾 حفظ")
        save_btn.setObjectName("successBtn")
        save_btn.clicked.connect(self.save_machine_safe)
        
        reset_btn = QPushButton("🔄 إعادة")
        reset_btn.setStyleSheet("background-color: #95a5a6; color: white; font-weight: bold; padding: 10px; border-radius: 5px;")
        reset_btn.clicked.connect(self.reset_form)
        
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(save_btn)
        btn_layout.addWidget(reset_btn)
        parts_layout.addLayout(btn_layout)
        
        parts_group.setLayout(parts_layout)
        self.form_layout.addWidget(parts_group)
        self.form_layout.addStretch()
        
        form_scroll.setWidget(form_container)
        layout.addWidget(form_scroll, 1)
        
        # الجانب الأيمن
        right_layout = QVBoxLayout()
        
        self.machine_table = QTableWidget()
        self.machine_table.setColumnCount(7)
        self.machine_table.setHorizontalHeaderLabels([
            "المصنع", "الصالة", "الخط", "المعدة", "الجزء", "الأمبير", "قراءات/يوم"
        ])
        self.machine_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.machine_table.setAlternatingRowColors(True)
        
        table_btn_layout = QHBoxLayout()
        delete_btn = QPushButton("🗑️ حذف")
        delete_btn.setObjectName("dangerBtn")
        delete_btn.clicked.connect(self.delete_machine)
        
        export_btn = QPushButton("📥 تصدير")
        export_btn.setObjectName("primaryBtn")
        export_btn.clicked.connect(self.export_data)
        
        table_btn_layout.addWidget(delete_btn)
        table_btn_layout.addWidget(export_btn)
        
        right_layout.addWidget(QLabel("📊 المعدات المسجلة:"))
        right_layout.addWidget(self.machine_table)
        right_layout.addLayout(table_btn_layout)
        
        layout.addLayout(right_layout, 2)
        
        self.tabs.addTab(tab, "⚙️ إدارة المعدات")
        self.refresh_machines_table()
    
    def add_part_row_safe(self):
        try:
            row_layout = QHBoxLayout()
            row_layout.setSpacing(8)
            
            name_input = QLineEdit()
            name_input.setPlaceholderText("موتور / مضخة / إلخ")
            name_input.setMinimumHeight(38)
            
            ampere_input = QDoubleSpinBox()
            ampere_input.setMinimum(0)
            ampere_input.setMaximum(9999)
            ampere_input.setDecimals(2)
            ampere_input.setSuffix(" A")
            ampere_input.setMinimumHeight(38)
            
            frequency_input = QSpinBox()
            frequency_input.setMinimum(1)
            frequency_input.setMaximum(100)
            frequency_input.setValue(1)
            frequency_input.setMinimumHeight(38)
            frequency_input.setFixedWidth(70)
            
            delete_btn = QPushButton("❌")
            delete_btn.setFixedSize(38, 38)
            delete_btn.setStyleSheet("background-color: #ffe5e5; color: #e74c3c; font-weight: bold; border: none; border-radius: 3px;")
            
            row_layout.addWidget(name_input, 2)
            row_layout.addWidget(ampere_input, 1)
            row_layout.addWidget(frequency_input, 1)
            row_layout.addWidget(delete_btn, 0)
            
            self.parts_form_layout.insertLayout(len(self.parts_rows), row_layout)
            
            row_data = PartRow(row_layout, name_input, ampere_input, frequency_input)
            self.parts_rows.append(row_data)
            
            delete_btn.clicked.connect(lambda: self.delete_part_row(row_data))
            logger.info(f"✅ تم إضافة صف جزء جديد")
            
        except Exception as e:
            logger.error(f"❌ خطأ في إضافة الصف: {e}", exc_info=True)
            QMessageBox.critical(self, "خطأ", f"فشل في إضافة الصف: {e}")
    
    def delete_part_row(self, row_data):
        if len(self.parts_rows) <= 1:
            QMessageBox.warning(self, "تنبيه", "يجب أن يكون هناك جزء واحد على الأقل")
            return
        
        while row_data.layout.count():
            item = row_data.layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        self.parts_form_layout.removeItem(row_data.layout)
        self.parts_rows.remove(row_data)
        logger.info("✅ تم حذف صف جزء")
    
    def save_machine_safe(self):
        try:
            factory = self.factory_input.text().strip()
            hall = self.hall_input.text().strip()
            line = self.line_input.text().strip()
            machine = self.machine_input.text().strip()
            
            if not factory or not machine:
                QMessageBox.warning(self, "تنبيه", "المصنع واسم المعدة مطلوبان")
                logger.warning("⚠️ محاولة حفظ بدون بيانات أساسية")
                return
            
            df = safe_read_csv(CONFIG_FILE)
            for row_data in self.parts_rows:
                name = row_data.name.text().strip()
                ampere = row_data.ampere.value()
                frequency = row_data.frequency.value()
                
                new_row = pd.DataFrame([[
                    factory, hall, line, machine, name, ampere, frequency
                ]], columns=['Factory', 'Hall', 'Line', 'Machine', 'Task', 'Normal_Val', 'Daily_Required'])
                
                df = pd.concat([df, new_row], ignore_index=True)
            
            safe_save_csv(df, CONFIG_FILE)
            self.refresh_machines_table()
            self.reset_form()
            
            logger.info(f"✅ تم حفظ المعدة: {machine} بـ {len(self.parts_rows)} أجزاء")
            QMessageBox.information(self, "نجاح", f"✅ تم حفظ المعدة '{machine}' بنجاح!\n\n🤖 البوت سيعرف البيانات الجديدة فوراً")
            
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ المعدة: {e}", exc_info=True)
            QMessageBox.critical(self, "خطأ", f"فشل الحفظ: {e}")
    
    def refresh_machines_table(self):
        try:
            df = safe_read_csv(CONFIG_FILE)
            self.machine_table.setRowCount(len(df))
            
            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    item = QTableWidgetItem(str(val))
                    self.machine_table.setItem(i, j, item)
            
            logger.debug(f"✅ تم تحديث جدول المعدات ({len(df)} صفوف)")
        except Exception as e:
            logger.error(f"❌ خطأ في تحديث الجدول: {e}", exc_info=True)
    
    def delete_machine(self):
        row = self.machine_table.currentRow()
        if row < 0:
            QMessageBox.warning(self, "تنبيه", "اختر صفاً أولاً")
            return
        
        reply = QMessageBox.question(self, "تأكيد", "هل تريد حذف هذه المعدة؟",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                df = safe_read_csv(CONFIG_FILE)
                df = df.drop(row).reset_index(drop=True)
                safe_save_csv(df, CONFIG_FILE)
                self.refresh_machines_table()
                logger.info(f"✅ تم حذف المعدة من الصف {row}")
            except Exception as e:
                logger.error(f"❌ خطأ في الحذف: {e}", exc_info=True)
    
    def reset_form(self):
        self.factory_input.clear()
        self.hall_input.clear()
        self.line_input.clear()
        self.machine_input.clear()
        
        for i in range(len(self.parts_rows) - 1, -1, -1):
            self.delete_part_row(self.parts_rows[i])
        
        self.add_part_row_safe()
        logger.info("🔄 تم إعادة تعيين النموذج")
    
    def export_data(self):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df = safe_read_csv(CONFIG_FILE)
            export_path = f"{DATA_DIR}/export_machines_{timestamp}.csv"
            safe_save_csv(df, export_path)
            QMessageBox.information(self, "نجاح", f"✅ تم التصدير إلى:\n{export_path}")
            logger.info(f"📥 تم تصدير البيانات: {export_path}")
        except Exception as e:
            logger.error(f"❌ خطأ في التصدير: {e}", exc_info=True)
    
    def setup_employees_tab(self):
        """تبويب الموظفين"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        form_group = QGroupBox("👥 إضافة موظف جديد")
        form_layout = QFormLayout()
        
        name_input = QLineEdit()
        name_input.setPlaceholderText("اسم الموظف الكامل")
        
        shift_combo = QComboBox()
        shift_combo.addItems(["الوردية الأولى ☀️", "الوردية الثانية 🌙"])
        
        form_layout.addRow("👤 الاسم:", name_input)
        form_layout.addRow("⏰ الوردية:", shift_combo)
        
        add_emp_btn = QPushButton("➕ إضافة موظف")
        add_emp_btn.setObjectName("successBtn")
        
        def add_employee():
            try:
                df = safe_read_csv(EMPLOYEES_FILE)
                new_emp = pd.DataFrame([[
                    0,
                    name_input.text().strip(),
                    shift_combo.currentText().split()[0],
                    "نشط",
                    datetime.now().strftime("%Y-%m-%d %H:%M")
                ]], columns=['Chat_ID', 'Employee_Name', 'Shift', 'Status', 'Registered_At'])
                
                df = pd.concat([df, new_emp], ignore_index=True)
                safe_save_csv(df, EMPLOYEES_FILE)
                
                logger.info(f"✅ تم إضافة موظف: {name_input.text()}")
                QMessageBox.information(self, "نجاح", "✅ تم إضافة الموظف")
                name_input.clear()
                self.refresh_employees_table()
            except Exception as e:
                logger.error(f"❌ خطأ في إضافة موظف: {e}", exc_info=True)
        
        add_emp_btn.clicked.connect(add_employee)
        form_layout.addRow(add_emp_btn)
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
        
        layout.addWidget(QLabel("👥 قائمة الموظفين:"))
        self.employees_table = QTableWidget()
        self.employees_table.setColumnCount(5)
        self.employees_table.setHorizontalHeaderLabels(["Chat ID", "الاسم", "الوردية", "الحالة", "التاريخ"])
        self.employees_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.employees_table.setAlternatingRowColors(True)
        layout.addWidget(self.employees_table)
        
        self.tabs.addTab(tab, "👥 الموظفون")
        self.refresh_employees_table()
    
    def refresh_employees_table(self):
        try:
            df = safe_read_csv(EMPLOYEES_FILE)
            self.employees_table.setRowCount(len(df))
            
            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    self.employees_table.setItem(i, j, QTableWidgetItem(str(val)))
            
            logger.debug(f"✅ تم تحديث جدول الموظفين ({len(df)} موظف)")
        except Exception as e:
            logger.error(f"❌ خطأ في تحديث جدول الموظفين: {e}", exc_info=True)
    
    def setup_tasks_tab(self):
        """تبويب المهام"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        form_group = QGroupBox("📋 إنشاء مهمة جديدة")
        form_layout = QFormLayout()
        
        machine_combo = QComboBox()
        df_config = safe_read_csv(CONFIG_FILE)
        if not df_config.empty:
            machines = (df_config['Machine'] + " (" + df_config['Factory'] + ")").unique().tolist()
            machine_combo.addItems(machines if machines else ["لا توجد معدات"])
        
        shift_combo = QComboBox()
        shift_combo.addItems(["الأولى", "الثانية"])
        
        due_time = QTimeEdit()
        due_time.setTime(QTime(12, 0))
        
        form_layout.addRow("⚙️ المعدة:", machine_combo)
        form_layout.addRow("⏰ الوردية:", shift_combo)
        form_layout.addRow("🕐 الموعد:", due_time)
        
        create_btn = QPushButton("✅ إنشاء مهمة")
        create_btn.setObjectName("successBtn")
        
        def create_task():
            try:
                if machine_combo.currentText() == "لا توجد معدات":
                    QMessageBox.warning(self, "تنبيه", "يجب إضافة معدات أولاً")
                    return
                
                df = safe_read_csv(TASKS_FILE)
                new_task = pd.DataFrame([[
                    len(df) + 1,
                    machine_combo.currentText(),
                    shift_combo.currentText(),
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                    due_time.time().toString(),
                    "معلقة",
                    "",
                    ""
                ]], columns=['Task_ID', 'Factory', 'Hall', 'Line', 'Machine',
                            'Shift', 'Created_At', 'Due_Time', 'Status', 'Assigned_To'])
                
                df = pd.concat([df, new_task], ignore_index=True)
                safe_save_csv(df, TASKS_FILE)
                logger.info(f"✅ تم إنشاء مهمة جديدة")
                QMessageBox.information(self, "نجاح", "✅ تم إنشاء المهمة")
                self.refresh_tasks_table()
            except Exception as e:
                logger.error(f"❌ خطأ في إنشاء المهمة: {e}", exc_info=True)
        
        create_btn.clicked.connect(create_task)
        form_layout.addRow(create_btn)
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
        
        layout.addWidget(QLabel("📋 المهام المسجلة:"))
        self.tasks_table = QTableWidget()
        self.tasks_table.setColumnCount(9)
        self.tasks_table.setHorizontalHeaderLabels(["ID", "المعدة", "الوردية", "التاريخ", "الموعد", "الحالة", "المسؤول", "الملاحظات", ""])
        self.tasks_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.tasks_table.setAlternatingRowColors(True)
        layout.addWidget(self.tasks_table)
        
        self.tabs.addTab(tab, "📋 المهام")
        self.refresh_tasks_table()
    
    def refresh_tasks_table(self):
        try:
            df = safe_read_csv(TASKS_FILE)
            self.tasks_table.setRowCount(len(df))
            
            for i, row in df.iterrows():
                for j in range(min(8, len(row))):
                    self.tasks_table.setItem(i, j, QTableWidgetItem(str(row.iloc[j])))
            
            logger.debug(f"✅ تم تحديث جدول المهام")
        except Exception as e:
            logger.error(f"❌ خطأ في تحديث المهام: {e}", exc_info=True)
    
    def setup_requests_tab(self):
        """تبويب الطلبات الدورية"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        form_group = QGroupBox("🔔 إعدادات الطلبات الدورية")
        form_layout = QFormLayout()
        
        machine_combo = QComboBox()
        df_config = safe_read_csv(CONFIG_FILE)
        if not df_config.empty:
            machines = (df_config['Machine'] + " (" + df_config['Factory'] + ")").unique().tolist()
            machine_combo.addItems(machines if machines else ["لا توجد معدات"])
        
        frequency_spin = QSpinBox()
        frequency_spin.setMinimum(5)
        frequency_spin.setMaximum(120)
        frequency_spin.setValue(15)
        frequency_spin.setSuffix(" دقيقة")
        
        form_layout.addRow("⚙️ المعدة:", machine_combo)
        form_layout.addRow("⏱️ التكرار:", frequency_spin)
        
        save_req_btn = QPushButton("✅ حفظ الطلب")
        save_req_btn.setObjectName("successBtn")
        
        def save_request():
            try:
                if machine_combo.currentText() == "لا توجد معدات":
                    QMessageBox.warning(self, "تنبيه", "يجب إضافة معدات أولاً")
                    return
                
                df = safe_read_csv(REQUESTS_FILE)
                new_req = pd.DataFrame([[
                    len(df) + 1,
                    machine_combo.currentText(),
                    "",
                    "",
                    "",
                    frequency_spin.value(),
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "نشط"
                ]], columns=['Request_ID', 'Factory', 'Hall', 'Line', 'Machine',
                            'Frequency_Minutes', 'Last_Sent', 'Status'])
                
                df = pd.concat([df, new_req], ignore_index=True)
                safe_save_csv(df, REQUESTS_FILE)
                logger.info(f"✅ تم حفظ طلب دوري جديد: {frequency_spin.value()} دقيقة")
                QMessageBox.information(self, "نجاح", f"✅ سيتم إرسال تنبيه كل {frequency_spin.value()} دقيقة")
                self.refresh_requests_table()
            except Exception as e:
                logger.error(f"❌ خطأ في حفظ الطلب: {e}", exc_info=True)
        
        save_req_btn.clicked.connect(save_request)
        form_layout.addRow(save_req_btn)
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
        
        layout.addWidget(QLabel("🔔 الطلبات الدورية:"))
        self.requests_table = QTableWidget()
        self.requests_table.setColumnCount(8)
        self.requests_table.setHorizontalHeaderLabels(["ID", "المصنع", "الصالة", "الخط", "المعدة", "التكرار", "آخر إرسال", "الحالة"])
        self.requests_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.requests_table.setAlternatingRowColors(True)
        layout.addWidget(self.requests_table)
        
        self.tabs.addTab(tab, "🔔 الطلبات الدورية")
        self.refresh_requests_table()
    
    def refresh_requests_table(self):
        try:
            df = safe_read_csv(REQUESTS_FILE)
            self.requests_table.setRowCount(len(df))
            
            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    self.requests_table.setItem(i, j, QTableWidgetItem(str(val)))
            
            logger.debug(f"✅ تم تحديث جدول الطلبات")
        except Exception as e:
            logger.error(f"❌ خطأ في تحديث الطلبات: {e}", exc_info=True)
    
    def setup_readings_tab(self):
        """تبويب القراءات"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        btn_layout = QHBoxLayout()
        refresh_btn = QPushButton("🔄 تحديث")
        refresh_btn.setObjectName("primaryBtn")
        refresh_btn.clicked.connect(self.refresh_readings_table)
        
        charts_btn = QPushButton("📊 الرسوم البيانية")
        charts_btn.setObjectName("successBtn")
        charts_btn.clicked.connect(lambda: webbrowser.open("http://127.0.0.1:5000"))
        
        btn_layout.addWidget(refresh_btn)
        btn_layout.addWidget(charts_btn)
        layout.addLayout(btn_layout)
        
        self.readings_table = QTableWidget()
        self.readings_table.setColumnCount(12)
        self.readings_table.setHorizontalHeaderLabels([
            "الوقت", "الموظف", "الوردية", "المصنع", "الصالة", "الخط",
            "المعدة", "المهمة", "القراءة", "الطبيعي", "الحيود", "موثق"
        ])
        self.readings_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.readings_table.setAlternatingRowColors(True)
        
        layout.addWidget(QLabel("📋 سجل القراءات:"))
        layout.addWidget(self.readings_table)
        
        self.tabs.addTab(tab, "📋 القراءات")
        self.refresh_readings_table()
    
    def refresh_readings_table(self):
        """تحديث جدول القراءات"""
        try:
            df = safe_read_csv(READINGS_FILE)
            if df.empty:
                return
            
            df = df.tail(100).iloc[::-1]
            self.readings_table.setRowCount(len(df))
            
            for i, (_, row) in enumerate(df.iterrows()):
                for j, val in enumerate(row):
                    item = QTableWidgetItem(str(val))
                    
                    # تلوين الحيود
                    if j == 10:
                        try:
                            deviation = float(val)
                            normal = float(row.iloc[9])
                            if deviation > normal * 0.15:
                                item.setBackground(QColor(255, 100, 100))
                                item.setForeground(QColor(255, 255, 255))
                            elif deviation > normal * 0.1:
                                item.setBackground(QColor(255, 200, 100))
                        except:
                            pass
                    
                    self.readings_table.setItem(i, j, item)
            
            logger.debug(f"✅ تم تحديث القراءات ({len(df)} سجل)")
        except Exception as e:
            logger.error(f"❌ خطأ في تحديث القراءات: {e}", exc_info=True)
    
    def setup_logs_tab(self):
        """تبويب اللوجات"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        btn_layout = QHBoxLayout()
        refresh_logs = QPushButton("🔄 تحديث")
        refresh_logs.setObjectName("primaryBtn")
        refresh_logs.clicked.connect(self.load_logs)
        
        clear_logs = QPushButton("🧹 مسح")
        clear_logs.setObjectName("dangerBtn")
        clear_logs.clicked.connect(self.clear_logs)
        
        open_logs = QPushButton("📁 فتح الملف")
        open_logs.clicked.connect(self.open_log_file)
        
        btn_layout.addWidget(refresh_logs)
        btn_layout.addWidget(clear_logs)
        btn_layout.addWidget(open_logs)
        layout.addLayout(btn_layout)
        
        self.logs_display = QTextEdit()
        self.logs_display.setReadOnly(True)
        self.logs_display.setStyleSheet("""
            background-color: #1e1e1e;
            color: #00ff00;
            font-family: 'Courier New';
            font-size: 10px;
        """)
        
        layout.addWidget(self.logs_display)
        self.tabs.addTab(tab, "📋 اللوجات")
        
        QTimer.singleShot(500, self.load_logs)
    
    def load_logs(self):
        try:
            if os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'r', encoding='utf-8') as f:
                    logs = f.read()
                    self.logs_display.setText(logs)
                    cursor = self.logs_display.textCursor()
                    cursor.movePosition(QTextCursor.MoveOperation.End)
                    self.logs_display.setTextCursor(cursor)
        except Exception as e:
            logger.error(f"❌ خطأ في تحميل اللوجات: {e}")
    
    def clear_logs(self):
        reply = QMessageBox.question(self, "تأكيد", "هل تريد مسح جميع اللوجات؟",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                with open(LOG_FILE, 'w', encoding='utf-8') as f:
                    f.write("")
                logger.info("🧹 تم مسح اللوجات")
                self.load_logs()
                QMessageBox.information(self, "نجاح", "✅ تم مسح اللوجات")
            except Exception as e:
                logger.error(f"❌ خطأ في مسح اللوجات: {e}")
    
    def open_log_file(self):
        try:
            if os.name == 'nt':
                os.startfile(LOG_FILE)
            else:
                subprocess.Popen(['xdg-open', LOG_FILE])
            logger.info("📁 تم فتح ملف اللوج")
        except Exception as e:
            logger.error(f"❌ خطأ: {e}")
    
    def toggle_bot(self):
        if self.bot_thread is None or not self.bot_thread.is_alive():
            self.start_bot()
        else:
            self.stop_bot()
    
    def start_bot(self):
        try:
            def run_bot():
                logger.info("🤖 جاري بدء البوت...")
                bot.polling(none_stop=True, timeout=90)
            
            def run_reminders():
                start_periodic_reminders()
            
            def run_flask():
                logger.info("📊 جاري بدء خادم التحليلات...")
                app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)
            
            self.bot_thread = threading.Thread(target=run_bot, daemon=True)
            self.bot_thread.start()
            
            reminders_thread = threading.Thread(target=run_reminders, daemon=True)
            reminders_thread.start()
            
            flask_thread = threading.Thread(target=run_flask, daemon=True)
            flask_thread.start()
            
            self.bot_status.setText("🤖 البوت: 🟢")
            self.bot_status.setStyleSheet("color: #2ecc71; font-size: 12px; font-weight: bold;")
            logger.info("✅ تم بدء جميع الخدمات")
            self.statusBar().showMessage("✅ البوت والتحليلات تعمل بنجاح - الرابط: http://127.0.0.1:5000", 5000)
            
        except Exception as e:
            logger.error(f"❌ خطأ في بدء البوت: {e}", exc_info=True)
            QMessageBox.critical(self, "خطأ", f"فشل في بدء البوت: {e}")
    
    def stop_bot(self):
        try:
            global stop_requests
            stop_requests = True
            logger.info("✅ جاري إيقاف البوت...")
            self.bot_status.setText("🤖 البوت: 🔴")
            self.bot_status.setStyleSheet("color: #e74c3c; font-size: 12px; font-weight: bold;")
        except Exception as e:
            logger.error(f"❌ خطأ في إيقاف البوت: {e}")
    
    def closeEvent(self, event):
        logger.info("👋 جاري إغلاق البرنامج...")
        self.stop_bot()
        event.accept()

# ========================== MAIN ==========================
if __name__ == '__main__':
    logger.info("=" * 80)
    logger.info("🏭 نظام إدارة الصيانة ��لموحد v3.0 - بدء التشغيل")
    logger.info("=" * 80)
    
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = MaintenanceApp()
    window.show()
    
    sys.exit(app.exec())