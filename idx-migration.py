import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch, helpers
from datetime import datetime
import csv

# --- ШАГ 1: Проверка соединения и подключение ---

# 1. Загружаем переменные из .env в окружение
load_dotenv()
# SQL аналог: CREATE TABLE _backup AS SELECT * FROM v3
source_idx = "upsrb_mortgage_memo_20251028"
target_idx = "upsrb_mortgage_memo_20260416"

# 2. Подключение

raw_host = os.getenv('OPENSEARCH_URL', '')
clean_host = raw_host.replace('https://', '').replace('http://', '').split(':')[0]

# 3. Инициализируем клиент OpenSearch
client = OpenSearch(
    hosts=[{'host': clean_host, 'port': int(os.getenv(f'OPENSEARCH_PORT', 9200))}],
    http_auth=(os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PASSWORD')),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)

def get_embedding(text):
    """
    Генерация векторного представления текста.

    ВРЕМЕННАЯ ЗАГЛУШКА: детерминированные псевдо-векторы на основе hash текста.
    TODO: Заменить на реальную модель (dimension=2560).
    Это лучше чем нули, так как дает разные векторы для разных текстов.
    """
    import hashlib

    # Создаем детерминированный вектор на основе хеша текста
    hash_obj = hashlib.sha256(text.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()

    # Преобразуем хеш в вектор размерности 2560
    vector = []
    for i in range(2560):
        # Используем байты хеша циклически
        byte_val = int(hash_hex[(i % 64):((i % 64) + 2)], 16)
        # Нормализуем в диапазон [-1, 1]
        normalized = (byte_val - 128) / 128.0
        vector.append(normalized)

    return vector

def validate_row(row):
    """
    Валидация строки CSV перед обработкой.
    Возвращает True если строка валидна, иначе False.
    """
    # Проверяем обязательные поля
    required_fields = ['Название раздела', 'Номер пункта']
    for field in required_fields:
        if not row.get(field) or row.get(field).strip() == '':
            print(f"ПРЕДУПРЕЖДЕНИЕ: Пропуск строки: отсутствует поле '{field}'")
            return False

    return True

def prepare_actions():
    file_path = "sources/Памятка_upsrb_mortgage_memo_20251028.csv"

    if not os.path.exists(file_path):
        print(f"ОШИБКА: Файл {file_path} не найден!")
        return

    processed_count = 0
    skipped_count = 0

    with open(file_path, mode='r', encoding='utf-8-sig') as f:  # utf-8-sig для BOM
        reader = csv.DictReader(f, delimiter=';')

        for i, row in enumerate(reader):
            # Валидация данных
            if not validate_row(row):
                skipped_count += 1
                continue

            # Извлекаем данные с дефолтными значениями
            chapter_num = row.get('Номер раздела', '').strip()
            chapter_desc = row.get('Название раздела', '').strip()
            paragraph_num = row.get('Номер пункта', '').strip()
            paragraph_desc = row.get('Краткое описание (вопрос)', '').strip()
            sub_num = row.get('Номер подпункта', '').strip()
            sub_title = row.get('Название подпункта', '').strip()
            sub_desc = row.get('Краткое описание подпункта', '').strip()
            clarification = row.get('Разъяснения/позиция ЮП', '').strip()
            conclusion = row.get('Вывод ЮП (ПЭ) в LegalForms', '').strip()

            # Формируем полный текст для поиска и векторизации
            full_text = (
                f"Раздел: {chapter_desc}. "
                f"Пункт: {paragraph_num} - {paragraph_desc}. "
                f"Подпункт: {sub_num} - {sub_title}. "
                f"Описание: {sub_desc}. "
                f"Разъяснения: {clarification}. "
                f"Вывод: {conclusion}"
            )

            processed_count += 1

            yield {
                "_index": target_idx,
                "_source": {
                    "text": full_text,
                    "vector": get_embedding(full_text),
                    "chapter": {
                        "num": chapter_num,
                        "description": chapter_desc
                    },
                    "paragraph": {
                        "num": paragraph_num,
                        "description": paragraph_desc,
                        "text": full_text
                    },
                    "subparagraph": {
                        "num": sub_num if sub_num else None,
                        "title": sub_title if sub_title else None,
                        "description": sub_desc if sub_desc else None,
                        "clarification": clarification if clarification else None,
                        "conclusion": conclusion if conclusion else None,
                        "text": full_text
                    },
                    "insert_date": datetime.now().isoformat()
                }
            }

    print(f"Статистика обработки: обработано={processed_count}, пропущено={skipped_count}")

# --- MAIN SCRIPT ---
def main():
    print("="*70)
    print("МИГРАЦИЯ ИНДЕКСА OPENSEARCH")
    print("="*70)

    if not client.ping():
        print("ОШИБКА: Нет связи с OpenSearch")
        return

    print(f"Подключение к OpenSearch установлено")

    # Проверяем существование исходного индекса
    if not client.indices.exists(index=source_idx):
        print(f"ОШИБКА: Исходный индекс {source_idx} не найден!")
        return

    print(f"Исходный индекс: {source_idx}")

    # Получаем структуру старого индекса
    source_info = client.indices.get(index=source_idx)
    current_mappings = source_info[source_idx]['mappings']
    current_settings = source_info[source_idx]['settings']

    # Очистка настроек от системных ID
    for key in ['creation_date', 'uuid', 'provided_name', 'version']:
        current_settings['index'].pop(key, None)

    # Удаляем целевой индекс если существует
    if client.indices.exists(index=target_idx):
        print(f"Индекс {target_idx} уже существует. Удаляем...")
        client.indices.delete(index=target_idx)
        print(f"Индекс {target_idx} удален")

    # Создаем новый индекс
    client.indices.create(index=target_idx, body={
        "mappings": current_mappings,
        "settings": current_settings
    })
    print(f"Индекс {target_idx} создан")

    # ---- ШАГ 2: Переливка данных (Reindex) ----
    print(f"\nШАГ 2: Переливка старых данных из {source_idx}...")
    try:
        reindex_result = client.reindex(body={
            "source": {"index": source_idx},
            "dest": {"index": target_idx}
        }, wait_for_completion=True)

        old_docs_count = reindex_result.get('created', 0) + reindex_result.get('updated', 0)
        print(f"Старые данные скопированы: {old_docs_count} документов")
    except Exception as e:
        print(f"ОШИБКА: Ошибка при переливке данных: {e}")
        return

    # --- ШАГ 3: Заливка новых данных из CSV ----
    print(f"\nШАГ 3: Загрузка новых данных из CSV...")

    # Предварительная проверка файла
    csv_file = "sources/Памятка_upsrb_mortgage_memo_20251028.csv"
    if not os.path.exists(csv_file):
        print(f"ОШИБКА: Файл {csv_file} не найден!")
        return

    try:
        success, failed = helpers.bulk(client, prepare_actions(), refresh=True)
        print(f"Добавлено новых документов: {success}")
        if isinstance(failed, list) and len(failed) > 0:
            print(f"Документов с ошибками: {len(failed)}")
            for error in failed[:3]:
                print(f"   {error}")
    except Exception as e:
        print(f"ОШИБКА: Ошибка при загрузке данных: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- ШАГ 4: Валидация ---
    print(f"\nШАГ 4: Итоговая проверка...")
    total_docs = client.count(index=target_idx)['count']
    new_docs = client.count(index=target_idx, body={"query": {"exists": {"field": "insert_date"}}})['count']

    print(f"\n{'='*70}")
    print(f"РЕЗУЛЬТАТЫ МИГРАЦИИ")
    print(f"{'='*70}")
    print(f"Индекс: {target_idx}")
    print(f"Всего документов: {total_docs}")
    print(f"Новых документов (с insert_date): {new_docs}")
    print(f"Старых документов (из {source_idx}): {total_docs - new_docs}")

    # Проверка качества данных
    print(f"\nПроверка качества данных:")

    # Проверяем наличие векторов
    vector_check = client.count(index=target_idx, body={
        "query": {"exists": {"field": "vector"}}
    })['count']
    print(f"   - Документов с векторами: {vector_check}/{total_docs}")

    # Проверяем структуру chapter
    chapter_check = client.count(index=target_idx, body={
        "query": {"exists": {"field": "chapter.num"}}
    })['count']
    print(f"   - Документов с chapter.num: {chapter_check}/{total_docs}")

    # Проверяем структуру subparagraph
    sub_check = client.count(index=target_idx, body={
        "query": {"exists": {"field": "subparagraph.num"}}
    })['count']
    print(f"   - Документов с subparagraph.num: {sub_check}/{total_docs}")

    if new_docs > 0 and chapter_check > 0:
        print(f"\nУСПЕХ: Миграция завершена успешно.")
        print(f"ВНИМАНИЕ: Используется заглушка для векторов!")
        print(f"   Замените get_embedding() на реальную модель для семантического поиска.")
    else:
        print(f"\nОШИБКА: Новые данные не загружены или структура неверна")

    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
