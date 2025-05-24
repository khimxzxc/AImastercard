import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1"
)

def get_segment_by_behavior(features: dict):
    try:
        prompt = f"""
Ты — интеллектуальный банковский помощник. Проанализируй клиента строго на русском языке.

Задачи:
1. Назови сегмент клиента
2. Объясни, почему он попал в этот сегмент
3. Составь таблицу с ключевыми метриками (в Markdown)
4. Предложи до 3 рекомендаций банку

Данные клиента:
- Транзакций: {features['total_txns']}
- Средний чек: {features['avg_txn_amt']:.0f}₸
- Еда: {features['pct_food']*100:.1f}%
- Travel: {features['pct_travel']*100:.1f}%
- Кошелёк: {'Да' if features['pct_wallet_use'] > 0.5 else 'Нет'}
- Зарплата: {'Да' if features['salary_flag'] else 'Нет'}
- Уникальных городов: {features['unique_cities']}

Сегменты:
1. Цифровой путешественник
2. Инвестирующий семьянин
3. Городской исследователь

Формат ответа:
segment_name: <название сегмента>
explanation: <обоснование>
metrics_table:
  | Метрика | Значение |
  |---------|----------|
  | ...     | ...      |
recommendation:
  - ...
  - ...
  - ...
"""

        response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )

        content = response.choices[0].message.content.strip()
        print("[DEBUG] LLaMA3 response:\n", content)

        lines = content.splitlines()
        name = next((l.split(":", 1)[1].strip() for l in lines if l.lower().startswith("segment_name")), "Неопределено")
        explanation = next((l.split(":", 1)[1].strip() for l in lines if l.lower().startswith("explanation")), "—")

        table_start = next((i for i, l in enumerate(lines) if 'metrics_table' in l.lower()), -1)
        table_lines = lines[table_start+1:] if table_start >= 0 else []
        table_lines = [l for l in table_lines if l.strip().startswith('|')]

        recommendations = [l.strip("- ").strip() for l in lines if l.strip().startswith("- ") and not l.strip().startswith("|")]

        return {
            "segment_name": name,
            "explanation": explanation,
            "metrics_markdown": '\n'.join(table_lines),
            "recommendation": recommendations
        }

    except Exception as e:
        print("[ERROR in LLaMA3 call]:", e)
        return {
            "segment_name": "Ошибка",
            "explanation": str(e),
            "metrics_markdown": "",
            "recommendation": []
        }
