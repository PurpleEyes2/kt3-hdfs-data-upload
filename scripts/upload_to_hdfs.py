from hdfs import InsecureClient
from pathlib import Path
import os

def upload_to_hdfs():
    # Настройки HDFS
    HDFS_URL = 'http://localhost:9870'
    HDFS_USER = 'root'
    HDFS_PATH = '/user/data/normalized/'
    
    # Локальный путь к данным
    LOCAL_DATA = Path(__file__).parent.parent / 'data' / 'processed'
    
    try:
        # Подключение к HDFS
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        
        # Создание директории в HDFS
        client.makedirs(HDFS_PATH)
        
        # Загрузка файлов
        for file in LOCAL_DATA.glob('*.parquet'):
            hdfs_file_path = os.path.join(HDFS_PATH, file.name)
            client.upload(hdfs_file_path, str(file))
            print(f'Uploaded: {file.name} to {hdfs_file_path}')
        
        print('\nHDFS Contents:')
        print(client.list(HDFS_PATH))
        
    except Exception as e:
        print(f'Error: {e}')

if __name__ == '__main__':
    upload_to_hdfs()