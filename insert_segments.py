import sqlite3

conn = sqlite3.connect("clients.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS segments (
  segment_id INTEGER PRIMARY KEY,
  segment_name TEXT,
  segment_description TEXT
)
""")

segments = [
    (0, "Городской исследователь", "Экономит на еде, транспорте и жилье в городе. Использует цифровые кошельки, активен в разных районах."),
    (1, "Инвестирующий семьянин", "Стабильный доход, покупки для семьи, тратит меньше на travel. Часто использует зарплатную карту."),
    (2, "Цифровой путешественник", "Проводит операции за границей, тратит на travel и еду. Использует ApplePay/GooglePay, не получает зарплату.")
]

cursor.executemany("INSERT OR REPLACE INTO segments VALUES (?, ?, ?)", segments)

conn.commit()
conn.close()

print("✅ Сегменты добавлены в базу данных.")
