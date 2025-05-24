import os
import logging
import sqlite3
import pandas as pd
from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from dotenv import load_dotenv
from groq_client import get_segment_by_behavior
from insight_chart import plot_behavior
import random

# Загрузка .env
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_PATH = "clients.db"

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я бот по клиентской сегментации.\n\n"
        "📌 Доступные команды:\n"
        "/segment <card_id> — покажу сегмент и поведение клиента\n"
        "/insight <card_id> — сгенерирую инсайт через AI и график\n"
        "/clients — список всех клиентов (в виде файла)\n"
        "/random — покажу случайного клиента\n"
        "/segments — статистика по сегментам"
    )

# /segment <card_id>
async def segment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("Используй: /segment <card_id>")
        return

    try:
        card_id = int(context.args[0])
        df = pd.read_parquet("client_features.parquet")
        row = df[df["card_id"] == card_id]
        if row.empty:
            await update.message.reply_text("⚠️ Клиент не найден в фичах.")
            return

        feats = row.iloc[0].to_dict()

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT segment_id FROM clients WHERE card_id = ?", (card_id,))
        seg_result = cur.fetchone()
        cur.execute("SELECT segment_name, segment_description FROM segments WHERE segment_id = ?", (seg_result[0],))
        seg_info = cur.fetchone()
        conn.close()

        seg_name = seg_info[0] if seg_info else "Неизвестно"
        seg_desc = seg_info[1] if seg_info else "Описание отсутствует."

        msg = f"📇 Клиент: {card_id}\n\n"
        msg += f"📊 Поведение:\n"
        msg += f"- Транзакций: {feats['total_txns']}\n"
        msg += f"- Средний чек: {feats['avg_txn_amt']:.0f}₸\n"
        msg += f"- Еда: {feats['pct_food']*100:.1f}%\n"
        msg += f"- Travel: {feats['pct_travel']*100:.1f}%\n"
        msg += f"- Кошелёк: {'Да' if feats['pct_wallet_use'] > 0.5 else 'Нет'}\n"
        msg += f"- Зарплата: {'Да' if feats['salary_flag'] else 'Нет'}\n"
        msg += f"- Уник. города: {feats['unique_cities']}\n\n"

        msg += f"🧩 Сегмент: {seg_name}\n"
        msg += f"📌 {seg_desc}"

        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при выводе информации о клиенте.")

# /insight <card_id>
async def insight(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("Используй: /insight <card_id>")
        return

    card_id = context.args[0]
    try:
        df = pd.read_parquet("client_features.parquet")
        row = df[df["card_id"] == int(card_id)]
        if row.empty:
            await update.message.reply_text("⚠️ Клиент не найден в фичах.")
            return

        features = row.iloc[0].drop("card_id").to_dict()
        await update.message.reply_text("🤖 Отправляю запрос в LLaMA3...")
        result = get_segment_by_behavior(features)

        msg = f"📌 Сегмент: {result['segment_name']}\n"
        msg += f"🧠 Обоснование: {result['explanation']}\n"

        if result['metrics_markdown']:
            msg += f"\n📊 Метрики:\n"
            msg += f"```\n{result['metrics_markdown']}\n```"

        if result['recommendation']:
            msg += f"\n✅ Рекомендации:\n"
            for r in result['recommendation']:
                msg += f"• {r}\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

        # график
        chart_path = plot_behavior(int(card_id), features)
        await update.message.reply_photo(photo=open(chart_path, "rb"))

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Произошла ошибка при генерации инсайта.")

# /clients
async def clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT card_id FROM clients", conn)
        conn.close()

        filepath = "clients_list.txt"
        df.to_csv(filepath, index=False, header=False)

        await update.message.reply_text("📎 Отправляю список всех клиентов (2 000+):")
        await update.message.reply_document(document=open(filepath, "rb"))
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при формировании списка клиентов.")

# /random
async def random_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT card_id, segment_id FROM clients", conn)
        conn.close()
        row = df.sample(1).iloc[0]
        await update.message.reply_text(f"🎲 Случайный клиент: {row['card_id']}\nСегмент: {row['segment_id']}")
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при выборе случайного клиента.")

# /segments
async def segments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT segment_id, COUNT(*) as count FROM clients GROUP BY segment_id ORDER BY count DESC", conn)
        conn.close()

        lines = [f"Сегмент {row['segment_id']}: {row['count']} клиентов" for _, row in df.iterrows()]
        message = "\n".join(lines)
        await update.message.reply_text(f"📊 Распределение по сегментам:\n{message}")
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при подсчёте сегментов.")

# Запуск
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("❌ TELEGRAM_BOT_TOKEN не найден в .env")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("segment", segment))
    app.add_handler(CommandHandler("insight", insight))
    app.add_handler(CommandHandler("clients", clients))
    app.add_handler(CommandHandler("random", random_client))
    app.add_handler(CommandHandler("segments", segments))

    print("✅ Бот запущен.")
    app.run_polling()
