import pandas as pd
import os

INPUT_FILE = "DECENTRATHON_3.0.parquet"
OUTPUT_FILE = "client_features.parquet"

print("[INFO] Загрузка данных из:", INPUT_FILE)
df = pd.read_parquet(INPUT_FILE)

print(f"[INFO] Всего строк: {len(df):,}")
print("[INFO] Очистка и обработка NA...")

# Обработка пропущенных значений
df["wallet_type"] = df["wallet_type"].fillna("NA")
df["merchant_city"] = df["merchant_city"].fillna("NA")

# Безопасно преобразуем merchant_mcc в числовой формат
df["merchant_mcc"] = pd.to_numeric(df["merchant_mcc"], errors="coerce").fillna(-1).astype(int)

# Агрегация поведенческих фичей
print("[INFO] Агрегируем поведение по card_id...")
client_features = df.groupby("card_id").agg(
    total_txns=("transaction_amount_kzt", "count"),
    avg_txn_amt=("transaction_amount_kzt", "mean"),
    pct_food=("merchant_mcc", lambda x: sum(mcc in [5411, 5812, 5814] for mcc in x) / len(x)),
    pct_travel=("merchant_mcc", lambda x: sum(mcc in [3000, 3351, 4511, 4722] for mcc in x) / len(x)),
    pct_wallet_use=("wallet_type", lambda x: (x != "NA").sum() / len(x)),
    salary_flag=("transaction_type", lambda x: int("SALARY" in x.values)),
    unique_cities=("merchant_city", pd.Series.nunique)
).reset_index()

# Создание папки, если нет
os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)

# Сохранение
client_features.to_parquet(OUTPUT_FILE, index=False)
print(f"[✅] Сохранено: {OUTPUT_FILE}")
print(f"[ℹ️] Клиентов обработано: {len(client_features):,}")
