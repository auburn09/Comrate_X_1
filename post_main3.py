import pandas as pd
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'log_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.txt'),
        logging.StreamHandler()
    ]
)

# Улучшенная предобработка текста
def preprocess_text(text):
    if pd.isna(text):
        return ''
    text = str(text).strip().lower()
    # Удаляем лишние пробелы между словами
    text = ' '.join(text.split())
    return text

# Загрузка данных
ao_db_prod = pd.read_csv('AO db prod.csv', delimiter=';')
mvdr_df = pd.read_csv('MVDR23_DEPARTMENTS_7UTF-8.csv', delimiter=';')

# Предобработка данных
ao_db_prod['name_ru'] = ao_db_prod['name_ru'].apply(preprocess_text)
ao_db_prod['regula_code'] = ao_db_prod['regula_code'].apply(preprocess_text)
mvdr_df['departmentname'] = mvdr_df['departmentname'].apply(preprocess_text)
mvdr_df['departmentcode'] = mvdr_df['departmentcode'].apply(preprocess_text)

# Логируем исходные данные
logging.info(f"Строк в AO db prod изначально: {len(ao_db_prod)}")
logging.info(f"Строк в MVDR23 изначально: {len(mvdr_df)}")
logging.info(f"Строк в AO db prod с непустым id до обработки: {len(ao_db_prod[ao_db_prod['id'].notna() & (ao_db_prod['id'] != '')])}")
duplicates = ao_db_prod.duplicated(subset=['name_ru', 'regula_code'], keep='first').sum()
logging.info(f"Найдено дубликатов в AO db prod по (name_ru, regula_code): {duplicates}")

# Создание словаря из MVDR23
mvdr_dict = {}
for _, row in mvdr_df.iterrows():
    key = (row['departmentname'], row['departmentcode'])
    if key in mvdr_dict:
        logging.warning(f"Дубликат в MVDR23: {key}, старый recordid={mvdr_dict[key]}, новый={row['recordid']}")
    mvdr_dict[key] = row['recordid']

logging.info(f"Словарь создан, размер: {len(mvdr_dict)} записи")

# Обработка строк AO db prod
ao_db_prod['epgu_code'] = ''
for index, row in ao_db_prod.iterrows():
    key = (row['name_ru'], row['regula_code'])
    if key in mvdr_dict:
        recordid = mvdr_dict[key]
        ao_db_prod.at[index, 'epgu_code'] = recordid
        logging.info(f"Строка id={row['id']}: присвоен epgu_code={recordid}")
    else:
        logging.warning(f"Строка id={row['id']}: не найдено совпадение для key={key}")

logging.info("Обработка строк AO db prod завершена")

# Диагностика
ao_with_epgu = len(ao_db_prod[ao_db_prod['epgu_code'].notna() & (ao_db_prod['epgu_code'] != '')])
logging.info(f"Строк в AO db prod с непустым epgu_code после обработки: {ao_with_epgu}")

# Добавление необработанных из MVDR23
processed_recordids = set(ao_db_prod['epgu_code'].dropna())
for _, row in mvdr_df.iterrows():
    if row['recordid'] not in processed_recordids:
        logging.info(f"Необработанная строка recordid={row['recordid']}: добавлена")
        ao_db_prod = pd.concat([ao_db_prod, pd.DataFrame([{
            'id': '',
            'name_ru': row['departmentname'],
            'name_en': '',
            'regula_code': row['departmentcode'],
            'elpost_code': '',
            'epgu_code': row['recordid'],
            'original_regula_code': row['departmentcode']
        }])], ignore_index=True)

# Итоговый результат
logging.info(f"Всего строк в результирующем файле: {len(ao_db_prod)}")
logging.info(f"Строк с непустым id: {len(ao_db_prod[ao_db_prod['id'].notna() & (ao_db_prod['id'] != '')])}")
logging.info(f"Строк с непустым epgu_code: {len(ao_db_prod[ao_db_prod['epgu_code'].notna() & (ao_db_prod['epgu_code'] != '')])}")
logging.info(f"Уникальных epgu_code: {ao_db_prod['epgu_code'].nunique()}")

# Выборка необработанных строк
unmatched_with_id = ao_db_prod[
    (ao_db_prod['id'].notna() & (ao_db_prod['id'] != '')) &
    (ao_db_prod['epgu_code'].isna() | (ao_db_prod['epgu_code'] == ''))
]
logging.info(f"Найдено строк с непустым id и пустым epgu_code из AO db prod: {len(unmatched_with_id)}")

# Сохранение результатов
ao_db_prod.to_csv('processed_ao_db_prod.csv', sep=';', index=False)
unmatched_with_id.to_csv('unmatched_with_id.csv', sep=';', index=False)
logging.info("Файлы сохранены: processed_ao_db_prod.csv, unmatched_with_id.csv")