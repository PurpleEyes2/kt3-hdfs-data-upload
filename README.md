# Отчет по проекту: Пайплайн обработки данных для HDFS

## Оглавление
1. [Описание проекта](#описание-проекта)
2. [Структура проекта](#структура-проекта)
3. [Требования](#требования)
4. [Установка и настройка](#установка-и-настройка)
5. [Выполнение работы](#выполнение-работы)
6. [Результаты](#результаты)

## Описание проекта
Проект реализует ETL-пайплайн для:
- Загрузки датасетов с Kaggle
- Очистки и нормализации данных
- Конвертации в формат Parquet
- Загрузки в HDFS

## Структура проекта
```
.
├── data/ # Данные (исключены из git)
│ ├── raw/ # Исходные данные
│ └── processed/ # Обработанные данные
├── scripts/
│ ├── normalize.ipynb # Ноутбук обработки
│ └── upload_to_hdfs.py # Скрипт загрузки
├── docs/ # Документация
├── docker-compose.yml # Конфигурация Hadoop
├── requirements.txt # Зависимости Python
└── README.md # Этот файл
```

## Требования
- Python 3.8+
- Docker
- Kaggle API key
- 4GB свободной памяти

## Установка и настройка
### 1. Подготовка окружения
```
git clone https://github.com/PurpleEyes2/kt3-hdfs-data-upload.git
cd kt3-hdfs-data-upload.git
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
### 2. Настройка Kaggle API
1. Скачайте ```kaggle.json``` из настроек аккаунта Kaggle
2. Поместите в:
   - Linux/Mac: ```~/.kaggle/```
   - Windows: ```C:\Users\<user>\.kaggle\```
3. Установите права:
```chmod 600 ~/.kaggle/kaggle.json```

## Выполнение работы
1. Запуск Hadoop-кластера
```docker-compose up -d```
2. Обработка данных
```jupyter notebook scripts/normalize.ipynb```
##### Основные этапы обработки:
```
# Нормализация названий колонок
df.columns = df.columns.str.lower().str.replace(r'[\W]+', '_', regex=True)

# Обработка дат
df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Удаление дубликатов
df = df.drop_duplicates()
```
3. Загрузка в HDFS
```python scripts/upload_to_hdfs.py```
##### Содержимое скрипта:
```
from hdfs import InsecureClient

client = InsecureClient('http://namenode:9870', user='root')
client.upload('/user/data/normalized/', 'data/processed/')
```

## Результаты
Проверка в HDFS
```docker exec -it namenode hdfs dfs -ls /user/data/normalized```

Вывод:
```
-rw-r--r--   3 root supergroup    9866653 /user/data/normalized/car_accident.parquet
-rw-r--r--   3 root supergroup     125583 /user/data/normalized/world_events.parquet
```
