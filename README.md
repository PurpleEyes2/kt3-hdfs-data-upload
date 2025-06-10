# Пайплайн обработки данных

Проект скачивает датасеты с Kaggle, нормализует их и загружает в HDFS.

## Инструкция по запуску:

1. Настройка окружения:
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt