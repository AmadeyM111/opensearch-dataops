"""
Форматирование ответов агента в один абзац.

Запуск: python3 test_agent_answers.py
"""

import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# Загрузка переменных
load_dotenv()

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

INDEX_NAME = "upsrb_mortgage_memo_20260416"

def get_agent_answer(query_text):
    """Получает ответ агента на вопрос в формате одного абзаца."""

    search_query = {
        "size": 1,
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
                    }
                ]
            }
        }
    }

    result = client.search(index=INDEX_NAME, body=search_query)

    if not result['hits']['hits']:
        return "Ответ не найден в базе знаний"

    best_match = result['hits']['hits'][0]
    source = best_match['_source']

    subparagraph = source.get('subparagraph', {})

    # Формируем ответ из clarification и conclusion
    clarification = subparagraph.get('clarification', '').strip()
    conclusion = subparagraph.get('conclusion', '').strip()

    # Предпочитаем clarificaton, так как он содержит полный контекст
    if clarification:
        # Если clarification уже содержит conclusion, не дублируем
        if conclusion and conclusion not in clarification:
            answer = f"{clarification} {conclusion}"
        else:
            answer = clarification
    elif conclusion:
        answer = conclusion
    else:
        # Если нет ни того ни другого, берем текст
        answer = source.get('text', '')[:500]

    # Очищаем ответ
    answer = answer.replace('\n', ' ').replace('\r', ' ')
    answer = ' '.join(answer.split())  # Удаляем лишние пробелы

    # Ограничиваем длину для читаемости
    if len(answer) > 800:
        answer = answer[:797] + "..."

    return answer

def main():
    print("=" * 80)
    print("ОТВЕТЫ АГЕНТА НА ТРИ ВОПРОСА")
    print("=" * 80)

    questions = [
        "В отношении ЗУ установлено ограничение ЗОУИТ, содержащее запрет строительства",
        "ЗУ расположен в границах приаэродромной территории (ПАТ)- номер подзоны не определен",
        "ЗУ расположен в границах приаэродромной территории (ПАТ)"
    ]

    for i, question in enumerate(questions, 1):
        print(f"\n{i}. Вопрос к агенту:")
        print(f"   {question}")
        print(f"\n   Ответ:")

        answer = get_agent_answer(question)
        print(f"   {answer}")

        print("\n" + "-" * 80)

if __name__ == "__main__":
    main()
