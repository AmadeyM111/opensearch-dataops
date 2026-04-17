"""
Скрипт для детальной проверки результата миграции.

Проверяет индекс upsrb_mortgage_memo_20260416 после миграции:
- Количество документов
- Структура данных
- Наличие всех полей
- Примеры документов
"""

import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch
import json

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

INDEX_TO_CHECK = "upsrb_mortgage_memo_20260416"


def main():
    print("="*70)
    print(f" ПРОВЕРКА ИНДЕКСА {INDEX_TO_CHECK}")
    print("="*70)

    if not client.ping():
        print(" Нет связи с OpenSearch")
        return

    # Проверка существования индекса
    if not client.indices.exists(index=INDEX_TO_CHECK):
        print(f" Индекс {INDEX_TO_CHECK} не найден!")
        return

    print(f" Индекс {INDEX_TO_CHECK} найден\n")

    # Статистика по количеству документов
    print(" СТАТИСТИКА ПО ДОКУМЕНТАМ:")
    print("-" * 70)

    total_docs = client.count(index=INDEX_TO_CHECK)['count']
    print(f"Всего документов: {total_docs}")

    # Проверка документов с insert_date (новые)
    new_docs = client.count(index=INDEX_TO_CHECK, body={
        "query": {"exists": {"field": "insert_date"}}
    })['count']
    print(f"Новых документов (с insert_date): {new_docs}")

    # Проверка старых документов
    old_docs = total_docs - new_docs
    print(f"Старых документов (из reindex): {old_docs}")

    # Проверка наличия векторов
    vector_docs = client.count(index=INDEX_TO_CHECK, body={
        "query": {"exists": {"field": "vector"}}
    })['count']
    print(f"Документов с векторами: {vector_docs}")

    # Проверка вложенных объектов
    chapter_num_docs = client.count(index=INDEX_TO_CHECK, body={
        "query": {"exists": {"field": "chapter.num"}}
    })['count']
    print(f"Документов с chapter.num: {chapter_num_docs}")

    paragraph_num_docs = client.count(index=INDEX_TO_CHECK, body={
        "query": {"exists": {"field": "paragraph.num"}}
    })['count']
    print(f"Документов с paragraph.num: {paragraph_num_docs}")

    subparagraph_num_docs = client.count(index=INDEX_TO_CHECK, body={
        "query": {"exists": {"field": "subparagraph.num"}}
    })['count']
    print(f"Документов с subparagraph.num: {subparagraph_num_docs}")

    # Получаем пример документа
    print(f"\n ПРИМЕР ДОКУМЕНТА:")
    print("-" * 70)

    # Сначала ищем новый документ
    new_doc_query = {
        "size": 1,
        "query": {"exists": {"field": "insert_date"}},
        "sort": [{"insert_date": {"order": "desc"}}]
    }

    result = client.search(index=INDEX_TO_CHECK, body=new_doc_query)

    if result['hits']['hits']:
        doc = result['hits']['hits'][0]
        source = doc['_source']

        print(f"ID документа: {doc['_id']}")
        print(f"\nПолный текст (первые 200 символов):")
        print(f"  {source.get('text', '')[:200]}...")

        print(f"\nСтруктура chapter:")
        chapter = source.get('chapter', {})
        print(f"  - num: {chapter.get('num')}")
        print(f"  - description: {chapter.get('description', '')[:100]}...")

        print(f"\nСтруктура paragraph:")
        paragraph = source.get('paragraph', {})
        print(f"  - num: {paragraph.get('num')}")
        print(f"  - description: {paragraph.get('description', '')[:100]}...")

        print(f"\nСтруктура subparagraph:")
        subparagraph = source.get('subparagraph', {})
        print(f"  - num: {subparagraph.get('num')}")
        print(f"  - title: {subparagraph.get('title', '')[:100]}...")
        print(f"  - description: {subparagraph.get('description', '')[:100]}...")
        print(f"  - clarification: {subparagraph.get('clarification', '')[:100]}...")
        print(f"  - conclusion: {subparagraph.get('conclusion', '')[:100]}...")

        print(f"\nВектор:")
        vector = source.get('vector', [])
        print(f"  - размерность: {len(vector)}")
        print(f"  - первые 5 значений: {vector[:5] if vector else 'N/A'}")

        print(f"\nМетаданные:")
        print(f"  - insert_date: {source.get('insert_date')}")

    # Итоговая проверка качества
    print(f"\n{'='*70}")
    print(f" ИТОГОВАЯ ПРОВЕРКА КАЧЕСТВА:")
    print(f"{'='*70}")

    issues = []

    if new_docs == 0:
        issues.append(" Нет новых документов (insert_date)")

    if vector_docs < total_docs:
        issues.append(f" Не все документы имеют вектора: {vector_docs}/{total_docs}")

    if chapter_num_docs < total_docs:
        issues.append(f" Не все документы имеют chapter.num: {chapter_num_docs}/{total_docs}")

    if paragraph_num_docs < total_docs:
        issues.append(f" Не все документы имеют paragraph.num: {paragraph_num_docs}/{total_docs}")

    if subparagraph_num_docs < total_docs:
        issues.append(f" Не все документы имеют subparagraph.num: {subparagraph_num_docs}/{total_docs}")

    if issues:
        print(" НАЙДЕНЫ ПРОБЛЕМЫ:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print(" ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    main()
