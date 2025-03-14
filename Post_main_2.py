import pandas as pd
import logging
import re
from datetime import datetime

# Настройка логирования с кодировкой cp1251
logging.basicConfig(
    filename=f'merge_files_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='cp1251'
)

# Функция предобработки строк с удалением спецсимволов (для name_ru и departmentname)
def preprocess_text(text):
    if pd.isna(text):
        return ''
    text = re.sub(r'[^A-Za-zА-Яа-я0-9\s]', '', str(text))
    text = re.sub(r'\s+', ' ', text.strip()).upper()
    return text

# Функция предобработки для кодов (regula_code и departmentcode)
def preprocess_code(text):
    if pd.isna(text):
        return ''
    return str(text).strip().upper()

# Чтение файлов
logging.info("Начало чтения файлов")
try:
    ao_db_prod = pd.read_csv('AO db prod.csv', sep=';', encoding='utf-8', dtype=str)
    mvdr23 = pd.read_csv('MVDR23_DEPARTMENTS_7UTF-8.csv', sep=';', encoding='utf-8', dtype=str)
    logging.info("Файлы успешно прочитаны")
except Exception as e:
    logging.error(f"Ошибка при чтении файлов: {e}")
    raise

# Добавляем маркер источника
ao_db_prod['source'] = 'AO_db_prod'
mvdr23['source'] = 'MVDR23'

# Диагностика: считаем строки с непустым id в AO db prod
initial_ao_with_id = len(ao_db_prod[ao_db_prod['id'].notna() & (ao_db_prod['id'] != '')])
logging.info(f"Строк в AO db prod с непустым id до обработки: {initial_ao_with_id}")

# Сохраняем исходные значения regula_code и departmentcode
ao_db_prod['original_regula_code'] = ao_db_prod['regula_code']
mvdr23['original_departmentcode'] = mvdr23['departmentcode']
mvdr23['original_departmentname'] = mvdr23['departmentname']

# Предобработка данных
logging.info("Начало предобработки данных")
ao_db_prod['name_ru'] = ao_db_prod['name_ru'].apply(preprocess_text)
ao_db_prod['regula_code'] = ao_db_prod['regula_code'].apply(preprocess_code)
mvdr23['departmentname'] = mvdr23['departmentname'].apply(preprocess_text)
mvdr23['departmentcode'] = mvdr23['departmentcode'].apply(preprocess_code)
logging.info("Предобработка завершена: спецсимволы удалены из названий, коды сохранены в исходном виде")

# Проверка дубликатов в AO db prod
duplicates_ao = ao_db_prod.duplicated(subset=['name_ru', 'regula_code'], keep='first').sum()
logging.info(f"Найдено дубликатов в AO db prod по (name_ru, regula_code): {duplicates_ao}")

# Создание словаря для поиска совпадений и сохранения исходных departmentname
logging.info("Создание словаря для поиска совпадений")
mvdr_dict = {(row['departmentname'], row['departmentcode']): (row['recordid'], row['original_departmentname']) 
             for _, row in mvdr23.iterrows()}
logging.info(f"Словарь создан, размер: {len(mvdr_dict)} записей")

# Обработка строк AO db prod и обновление epgu_code
logging.info("Начало обработки строк AO db prod")
matched_ids = set()
used_keys = set()
for index, row in ao_db_prod.iterrows():
    key = (row['name_ru'], row['regula_code'])
    match = mvdr_dict.get(key)
    if match:
        recordid, _ = match
        if recordid not in matched_ids:
            ao_db_prod.at[index, 'epgu_code'] = recordid
            matched_ids.add(recordid)
            used_keys.add(key)
            logging.info(f"Строка id={row['id']}: найдено совпадение с recordid={recordid}")
        else:
            logging.warning(f"Строка id={row['id']}: дубликат, recordid={recordid} уже использован")
    else:
        logging.warning(f"Строка id={row['id']}: совпадение не найдено для name_ru='{row['name_ru']}', regula_code='{row['regula_code']}'")
logging.info("Обработка строк AO db prod завершена")

# Диагностика: считаем строки с epgu_code в AO db prod после обработки
ao_with_epgu = len(ao_db_prod[ao_db_prod['epgu_code'].notna() & (ao_db_prod['epgu_code'] != '')])
logging.info(f"Строк в AO db prod с непустым epgu_code после обработки: {ao_with_epgu}")

# Сохраняем обработанный AO db prod
ao_db_prod_processed = ao_db_prod.copy()

# Поиск необработанных строк из MVDR23
logging.info("Поиск необработанных строк из MVDR23")
unprocessed_mvdr23 = mvdr23[~mvdr23['recordid'].isin(matched_ids)].copy()
logging.info(f"Найдено необработанных строк из MVDR23: {len(unprocessed_mvdr23)}")

# Преобразование необработанных строк в формат AO db prod
if not unprocessed_mvdr23.empty:
    logging.info("Форматирование необработанных строк")
    unprocessed_formatted = pd.DataFrame({
        'id': [''] * len(unprocessed_mvdr23),
        'name_ru': unprocessed_mvdr23['departmentname'],
        'name_en': ['nan'] * len(unprocessed_mvdr23),
        'regula_code': unprocessed_mvdr23['original_departmentcode'],
        'elpost_code': [''] * len(unprocessed_mvdr23),
        'epgu_code': unprocessed_mvdr23['recordid'],
        'source': unprocessed_mvdr23['source']
    })
    for index, row in unprocessed_mvdr23.iterrows():
        logging.info(f"Необработанная строка recordid={row['recordid']}: добавлена как name_ru='{row['departmentname']}', epgu_code='{row['recordid']}'")
    final_data = pd.concat([ao_db_prod_processed, unprocessed_formatted], ignore_index=True)
    logging.info("Необработанные строки добавлены в итоговый результат")
else:
    final_data = ao_db_prod_processed
    logging.info("Необработанных строк не найдено")

# Заменяем regula_code на исходные значения
final_data['regula_code'] = final_data['original_regula_code'].fillna(final_data['regula_code'])
final_data = final_data.drop(columns=['original_regula_code'], errors='ignore')

# Распределяем исходные departmentname по epgu_code
logging.info("Распределение исходных departmentname из MVDR23")
recordid_to_departmentname = {row['recordid']: row['original_departmentname'] for _, row in mvdr23.iterrows()}
final_data['name_ru'] = final_data['epgu_code'].map(recordid_to_departmentname).fillna(final_data['name_ru'])
logging.info("Исходные departmentname успешно распределены")

# Постобработка: сортировка по id без изменения типа
logging.info("Сортировка данных по полю id")
final_data['id_sort'] = pd.to_numeric(final_data['id'], errors='coerce')
final_data = final_data.sort_values(by='id_sort', na_position='last')
final_data = final_data.drop(columns=['id_sort'])
logging.info("Сортировка завершена")

# Подсчёт статистики
logging.info("Подсчёт статистики")
initial_ao_rows = len(ao_db_prod)
initial_mvdr_rows = len(mvdr23)
total_final_rows = len(final_data)
rows_with_id = len(final_data[final_data['id'].notna() & (final_data['id'] != '')])
rows_with_elpost = len(final_data[final_data['elpost_code'].notna() & (final_data['elpost_code'] != '')])
rows_with_epgu = final_data['epgu_code'].notna().sum()
unique_epgu = final_data['epgu_code'].nunique()
matched_rows = len(matched_ids)

logging.info(f"Статистика:")
logging.info(f" - Строк в AO db prod изначально: {initial_ao_rows}")
logging.info(f" - Строк в MVDR23 изначально: {initial_mvdr_rows}")
logging.info(f" - Всего строк в результирующем файле: {total_final_rows}")
logging.info(f" - Строк с непустым id: {rows_with_id}")
logging.info(f" - Строк с непустым elpost_code: {rows_with_elpost}")
logging.info(f" - Строк с непустым epgu_code: {rows_with_epgu}")
logging.info(f" - Уникальных epgu_code: {unique_epgu}")
logging.info(f" - Строк успешно объединено: {matched_rows}")

# Выборка строк с непустым id и пустым epgu_code только из AO db prod
logging.info("Выборка строк с непустым id, которым не удалось присвоить epgu_code")
unmatched_with_id = ao_db_prod_processed[
    (ao_db_prod_processed['id'].notna() & (ao_db_prod_processed['id'] != '')) & 
    (ao_db_prod_processed['epgu_code'].isna() | (ao_db_prod_processed['epgu_code'] == ''))
]
unmatched_count = len(unmatched_with_id)
logging.info(f"Найдено строк с непустым id и пустым epgu_code из AO db prod: {unmatched_count}")

# Сохранение этих строк в отдельный файл
unmatched_file = 'unmatched_with_id.csv'
logging.info(f"Сохранение строк с непустым id и пустым epgu_code в файл {unmatched_file}")
try:
    unmatched_with_id.drop(columns=['source'], errors='ignore').to_csv(unmatched_file, sep=';', index=False, encoding='utf-8')
    logging.info(f"Строки успешно сохранены в {unmatched_file}")
except Exception as e:
    logging.error(f"Ошибка при сохранении файла {unmatched_file}: {e}")
    raise

# Удаляем временный столбец source из итогового результата
final_data = final_data.drop(columns=['source'], errors='ignore')

# Сохранение основного результата
output_file = 'result_file.csv'
logging.info(f"Сохранение результата в файл {output_file}")
try:
    final_data.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    logging.info("Результат успешно сохранён")
except Exception as e:
    logging.error(f"Ошибка при сохранении файла: {e}")
    raise

print(f"Обработка завершена. Результат сохранён в '{output_file}'. Строки с непустым id и пустым epgu_code сохранены в '{unmatched_file}'. Лог сохранён в файл.")