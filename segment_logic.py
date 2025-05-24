import pandas as pd
import numpy as np
import sqlite3
import os
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Пути к файлам
FEATURES_PATH = "client_features.parquet"
SQLITE_DB = "clients.db"

# Загружаем фичи
print("[INFO] Загружаем данные из:", FEATURES_PATH)
df = pd.read_parquet(FEATURES_PATH)

# Обрабатываем NA
df.fillna(0, inplace=True)

# Сохраняем card_id отдельно
card_ids = df['card_id']
X = df.drop(columns=['card_id'])

# Стандартизируем фичи
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Обучаем KMeans
print("[INFO] Обучаем KMeans (n_clusters=3)")
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
labels = kmeans.fit_predict(X_scaled)

# Показываем распределение
print("[DEBUG] Кластеры:", np.unique(labels, return_counts=True))

# Добавляем сегменты в DataFrame
df['segment_id'] = labels
df['card_id'] = card_ids

# Сохраняем в SQLite
print("[INFO] Сохраняем сегменты в SQLite:", SQLITE_DB)
dir_path = os.path.dirname(SQLITE_DB)
if dir_path:
    os.makedirs(dir_path, exist_ok=True)
conn = sqlite3.connect(SQLITE_DB)
df[['card_id', 'segment_id']].to_sql("clients", conn, if_exists="replace", index=False)
conn.close()

print("[INFO] Готово. Кол-во сегментов:", len(set(labels)))
