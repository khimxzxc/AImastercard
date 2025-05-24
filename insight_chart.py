import matplotlib.pyplot as plt

def plot_behavior(card_id, features):
    labels = ['Еда (%)', 'Travel (%)', 'Средний чек (тыс.₸)', 'Городов']
    values = [
        features['pct_food'] * 100,
        features['pct_travel'] * 100,
        features['avg_txn_amt'] / 1000,
        features['unique_cities']
    ]

    colors = ['#4CAF50', '#2196F3', '#FFC107', '#9C27B0']

    plt.figure(figsize=(6, 4))
    plt.bar(labels, values, color=colors)
    plt.title(f'Профиль клиента {card_id}')
    plt.tight_layout()

    path = f'chart_{card_id}.png'
    plt.savefig(path)
    plt.close()
    return path
