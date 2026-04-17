import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch
import json
from datetime import datetime

# Загрузка переменных
load_dotenv()

INDEX_NAME = "upsrb_mortgage_memo_20260416"

# Три вопроса для тестирования
test_queries = [
    {
        "query": "В отношении ЗУ установлено ограничение ЗОУИТ, содержащее запрет строительства",
        "difficulty": "простой"
    },
    {
        "query": "ЗУ расположен в границах приаэродромной территории (ПАТ)- номер подзоны не определен",
        "difficulty": "средний"
    },
    {
        "query": "ЗУ расположен в границах приаэродромной территории (ПАТ) - 3 подзона",
        "difficulty": "сложный"
    }
]

# Подключение к OpenSearch
raw_host = os.getenv('OPENSEARCH_URL', '')
clean_host = raw_host.replace('https://', '').replace('http://', '').split(':')[0]

client = OpenSearch(
    hosts=[{'host': clean_host, 'port': int(os.getenv('OPENSEARCH_PORT', 9200))}],
    http_auth=(os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PASSWORD')),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)


def search_documents(query_text, size=3):
    """Поиск документов по запросу."""

    search_query = {
        "size": size,
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "subparagraph.conclusion": {
                                "query": query_text,
                                "boost": 5.0
                            }
                        }
                    },
                    {
                        "match": {
                            "subparagraph.clarification": {
                                "query": query_text,
                                "boost": 4.0
                            }
                        }
                    },
                    {
                        "match": {
                            "subparagraph.title": {
                                "query": query_text,
                                "boost": 3.0
                            }
                        }
                    },
                    {
                        "match": {
                            "paragraph.description": {
                                "query": query_text,
                                "boost": 2.0
                            }
                        }
                    },
                    {
                        "match": {
                            "text": {
                                "query": query_text,
                                "boost": 1.0
                            }
                        }
                    }
                ]
            }
        }
    }

    result = client.search(index=INDEX_NAME, body=search_query)
    return result['hits']['hits']

def format_document(hit):
    """Форматирует документ для вывода."""
    source = hit.get('_source', {})
    score = hit.get('_score', 0)

    # Сбор базовой информации (Score и метка новизны)
    output = []
    output.append(f"Score: {score:.2f}")
    if source.get('insert_date'):
        output.append(f"[НОВЫЙ ДОКУМЕНТ]")
    
    # 2. Безопасное извлечение вложенных объектов
    ch = source.get('chapter', {})
    p = source.get('paragraph', {})
    sp = source.get('subparagraph', {})

    # 3. Линейная сборка строк (чистая логика вывода)
    if desc := ch.get('description'):
        output.append(f"Раздел: {ch.get('num', '?')} - {desc}")

    if desc := p.get('description'):
        output.append(f"Пункт: {p.get('num', '?')} - {desc}")

    if title := sp.get('title'):
        output.append(f"Подпункт: {sp.get('num', '?')} - {title}")

    # 4. Работа с длинными текстами
    if clar := sp.get('clarification'):
        output.append(f"Разъяснение: {clar[:200]}...")

    if conc := sp.get('conclusion'):
        output.append(f"Вывод: {conc}")

    return "\n  ".join(output)

def save_agent_output(data, output_dir="agent_outputs", prefix="search_result"):
    """Сохраняем результат работы агента в файл с timestampt"""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{prefix}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        # если передана строка - оборачиваем в структуру, иначе пишем как есть
        payload = {"response": data} if isinstance(data, str) else data
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Результаты сохранены: {os.path.abspath(filepath)}")
    return filepath

def main():
    print("=" * 80)
    print(f"ТЕСТИРОВАНИЕ ИНДЕКСА: {INDEX_NAME}")
    print("=" * 80)

    # Проверка индекса
    if not client.indices.exists(index=INDEX_NAME):
        print(f"ОШИБКА: Индекс {INDEX_NAME} не найден!")
        return

    total_docs = client.count(index=INDEX_NAME)['count']
    new_docs = client.count(index=INDEX_NAME, body={
        "query": {"exists": {"field": "insert_date"}}
    })['count']

    print(f"\nСтатистика индекса:")
    print(f"  Всего документов: {total_docs}")
    print(f"  Новых документов: {new_docs}")
    print(f"  Старых документов: {total_docs - new_docs}")

    all_test_results = []

    print("\n" + "=" * 80)
    print("ЗАПРОСЫ К АГЕНТУ")
    print("=" * 80)

    for i, test_item in enumerate(test_queries, 1):
        query = test_item['query']
        difficulty = test_item['difficulty']

        hits = search_documents(query, size=3)

        # Формируем структуру для сохранения
        query_result = {
            "query": query,
            "difficulty": test_item['difficulty'],
            "results": [hit['_source'] for hit in hits],
            "timestamp": datetime.now().isoformat()
        }
        all_test_results.append(query_result)

        print(f"\n{i}. Вопрос: {query}")
        print(f"   Сложность: {difficulty}")
        print("-" * 80)

        # Поиск документов
        if not hits:
            print("   НИЧЕГО НЕ НАЙДЕНО")
            continue

        print(f"   Найдено документов: {len(hits)}\n")

        for j, hit in enumerate(hits, 1):
            print(f"   Документ #{j}:")
            formatted = format_document(hit)
            for line in formatted.split("\n"):
                print(f"     {line}")
            print()

        print("=" * 80)

    # Сохраняем все в файл
    save_agent_output(all_test_results, prefix="mortgage_test")
    print("\n ОТЧЕТ СОХРАНЕН В agent_outputs/")
    print("ГОТОВО")

if __name__ == "__main__":
    main()
