"""
Тестовый скрипт для проверки миграции на малом наборе данных.

Создает тестовый индекс, загружает 5-10 документов из CSV и проверяет:
1. Корректность структуры данных
2. Наличие всех полей
3. Размерность векторов
4. Вложенные объекты (chapter, paragraph, subparagraph)
"""

import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch, helpers
from datetime import datetime
import csv
import hashlib

# Загрузка переменных окружения
load_dotenv()

raw_host = os.getenv('OPENSEARCH_URL', '')
clean_host = raw_host.replace('https://', '').replace('http://', '').split(':')[0]

# Подключение к OpenSearch
client = OpenSearch(
    hosts=[{'host': clean_host, 'port': int(os.getenv('OPENSEARCH_PORT', 9200))}],
    http_auth=(os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PASSWORD')),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)

TEST_INDEX = "test_migrations_check"
CSV_FILE = "sources/Памятка_upsrb_mortgage_memo_20251028.csv"
MAX_DOCS = 5  # Тестируем только первые 5 документов


def get_test_embedding(text):
    """Тестовая заглушка для векторов (детерминированная)."""
    hash_obj = hashlib.sha256(text.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()

    vector = []
    for i in range(2560):
        byte_val = int(hash_hex[(i % 64):((i % 64) + 2)], 16)
        normalized = (byte_val - 128) / 128.0
        vector.append(normalized)

    return vector


def validate_document_structure(doc):
    """Проверяет структуру документа."""
    issues = []

    required_fields = ['text', 'vector', 'chapter', 'paragraph', 'subparagraph']
    for field in required_fields:
        if field not in doc:
            issues.append(f"❌ Отсутствует поле: {field}")

    # Проверка chapter
    if 'chapter' in doc:
        if not isinstance(doc['chapter'], dict):
            issues.append(f"❌ chapter должен быть объектом, получил {type(doc['chapter'])}")
        else:
            if 'num' not in doc['chapter']:
                issues.append("❌ chapter.num отсутствует")
            if 'description' not in doc['chapter']:
                issues.append("❌ chapter.description отсутствует")

    # Проверка paragraph
    if 'paragraph' in doc:
        if not isinstance(doc['paragraph'], dict):
            issues.append(f"❌ paragraph должен быть объектом, получил {type(doc['paragraph'])}")
        else:
            for subfield in ['num', 'description', 'text']:
                if subfield not in doc['paragraph']:
                    issues.append(f"❌ paragraph.{subfield} отсутствует")

    # Проверка subparagraph
    if 'subparagraph' in doc:
        if not isinstance(doc['subparagraph'], dict):
            issues.append(f"❌ subparagraph должен быть объектом, получил {type(doc['subparagraph'])}")
        else:
            for subfield in ['num', 'title', 'description', 'clarification', 'conclusion', 'text']:
                if subfield not in doc['subparagraph']:
                    issues.append(f"⚠️ subparagraph.{subfield} отсутствует (может быть None)")

    # Проверка vector
    if 'vector' in doc:
        if not isinstance(doc['vector'], list):
            issues.append(f"❌ vector должен быть списком, получил {type(doc['vector'])}")
        elif len(doc['vector']) != 2560:
            issues.append(f"❌ Неверная размерность vector: {len(doc['vector'])} вместо 2560")

    return issues


def prepare_test_documents():
    """Подготавливает тестовые документы из CSV."""
    if not os.path.exists(CSV_FILE):
        print(f"❌ Файл {CSV_FILE} не найден!")
        return

    docs = []
    with open(CSV_FILE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')

        for i, row in enumerate(reader):
            if i >= MAX_DOCS:
                break

            # Извлекаем данные
            chapter_num = row.get('Номер раздела', '').strip()
            chapter_desc = row.get('Название раздела', '').strip()
            paragraph_num = row.get('Номер пункта', '').strip()
            paragraph_desc = row.get('Краткое описание (вопрос)', '').strip()
            sub_num = row.get('Номер подпункта', '').strip()
            sub_title = row.get('Название подпункта', '').strip()
            sub_desc = row.get('Краткое описание подпункта', '').strip()
            clarification = row.get('Разъяснения/позиция ЮП', '').strip()
            conclusion = row.get('Вывод ЮП (ПЭ) в LegalForms', '').strip()

            full_text = (
                f"Раздел: {chapter_desc}. "
                f"Пункт: {paragraph_num} - {paragraph_desc}. "
                f"Подпункт: {sub_num} - {sub_title}. "
                f"Описание: {sub_desc}. "
                f"Разъяснения: {clarification}. "
                f"Вывод: {conclusion}"
            )

            doc = {
                "_index": TEST_INDEX,
                "_source": {
                    "text": full_text,
                    "vector": get_test_embedding(full_text),
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

            docs.append(doc)

    return docs


def main():
    print("="*70)
    print("🧪 ТЕСТИРОВАНИЕ МИГРАЦИИ")
    print("="*70)

    if not client.ping():
        print("❌ Нет связи с OpenSearch")
        return

    print(f"✅ Подключение к OpenSearch установлено\n")

    # Удаляем тестовый индекс если существует
    if client.indices.exists(index=TEST_INDEX):
        print(f"🗑️ Удаляем существующий тестовый индекс {TEST_INDEX}")
        client.indices.delete(index=TEST_INDEX)

    # Получаем структуру из основного индекса
    source_idx = "upsrb_mortgage_memo_20251028"
    if not client.indices.exists(index=source_idx):
        print(f"❌ Исходный индекс {source_idx} не найден!")
        return

    source_info = client.indices.get(index=source_idx)
    mappings = source_info[source_idx]['mappings']
    settings = source_info[source_idx]['settings']

    # Очистка настроек
    for key in ['creation_date', 'uuid', 'provided_name', 'version']:
        settings['index'].pop(key, None)

    # Создаем тестовый индекс
    client.indices.create(index=TEST_INDEX, body={
        "mappings": mappings,
        "settings": settings
    })
    print(f"✅ Тестовый индекс {TEST_INDEX} создан\n")

    # Подготавливаем тестовые документы
    print(f"📄 Чтение {MAX_DOCS} документов из {CSV_FILE}...")
    test_docs = prepare_test_documents()
    print(f"✅ Прочитано {len(test_docs)} документов\n")

    # Валидация структуры перед загрузкой
    print("🔍 Проверка структуры документов:")
    all_valid = True
    for i, doc in enumerate(test_docs):
        issues = validate_document_structure(doc['_source'])
        if issues:
            print(f"\n📄 Документ #{i+1}:")
            for issue in issues:
                print(f"  {issue}")
            all_valid = False
        else:
            print(f"✅ Документ #{i+1}: структура корректна")

    if not all_valid:
        print("\n❌ Найдены проблемы со структурой данных!")
        return

    print("\n📤 Загрузка документов в OpenSearch...")
    success, failed = helpers.bulk(client, test_docs, refresh=True)

    if failed:
        print(f"❌ Ошибки при загрузке: {len(failed)} документов")
        for error in failed[:3]:  # Показываем первые 3 ошибки
            print(f"   - {error}")
        return

    print(f"✅ Успешно загружено: {success} документов\n")

    # Проверка загруженных данных
    print("🔍 Проверка загруженных данных:")
    total = client.count(index=TEST_INDEX)['count']
    print(f"   - Всего документов в индексе: {total}")

    # Получаем первый документ для проверки
    result = client.search(index=TEST_INDEX, body={"size": 1})
    if result['hits']['hits']:
        sample = result['hits']['hits'][0]['_source']
        print(f"\n📄 Пример загруженного документа:")
        print(f"   - text: {sample.get('text', '')[:100]}...")
        print(f"   - chapter.num: {sample.get('chapter', {}).get('num')}")
        print(f"   - chapter.description: {sample.get('chapter', {}).get('description', '')[:50]}...")
        print(f"   - paragraph.num: {sample.get('paragraph', {}).get('num')}")
        print(f"   - subparagraph.num: {sample.get('subparagraph', {}).get('num')}")
        print(f"   - vector dimension: {len(sample.get('vector', []))}")

        # Проверка полей
        has_insert_date = 'insert_date' in sample
        print(f"   - insert_date: {'✅' if has_insert_date else '❌'}")

    # Итог
    print(f"\n{'='*70}")
    print(f"✅ ТЕСТ ПРОЙДЕН УСПЕШНО!")
    print(f"{'='*70}")
    print(f"\n💡 Рекомендации:")
    print(f"   1. Структура данных корректна")
    print(f"   2. Можно запускать полную миграцию: python idx-migration.py")
    print(f"   3. После миграции замените get_embedding() на реальную модель")

    # Очистка
    print(f"\n🗑️ Удаление тестового индекса {TEST_INDEX}...")
    client.indices.delete(index=TEST_INDEX)
    print(f"✅ Тестовый индекс удален\n")


if __name__ == "__main__":
    main()
