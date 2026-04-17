"""
Простой скрипт для тестирования интеграции.

Пример использования:
    python test_integration.py
"""

import os
import sys
import logging

# Добавляем src в path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dotenv import load_dotenv

# Загружаем .env
load_dotenv()

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_kplus_client():
    """Тест API клиента Консультант+."""
    logger.info("=" * 60)
    logger.info("ТЕСТ 1: API клиент Консультант+")
    logger.info("=" * 60)

    try:
        from src.k_plus_agent import KPlusAgentClient

        client = KPlusAgentClient()
        logger.info(f"Клиент инициализирован: base_url={client.base_url}")

        # Тест отправки сообщения
        response = client.send_message("Какие документы нужны для ипотеки?")

        logger.info(f"Ответ получен:")
        logger.info(f"  - Контент: {response.content[:100] if response.content else 'Нет'}...")
        logger.info(f"  - Источников: {len(response.sources) if response.sources else 0}")
        logger.info(f"  - Заглушка: {response.plug_no_answer_k_plus if response.plug_no_answer_k_plus else 'Нет'}")

        return True

    except Exception as e:
        logger.error(f"Ошибка при тестировании клиента К+: {e}")
        return False


def test_opensearch_client():
    """Тест OpenSearch клиента."""
    logger.info("\n" + "=" * 60)
    logger.info("ТЕСТ 2: OpenSearch клиент")
    logger.info("=" * 60)

    try:
        from src.infrastructure.opensearch import get_client

        client = get_client()
        logger.info(f"Клиент OpenSearch инициализирован")

        # Проверяем подключение
        info = client.info()
        logger.info(f"Подключение успешно: {info['version']['number']}")

        return True

    except Exception as e:
        logger.error(f"Ошибка при тестировании OpenSearch: {e}")
        return False


def test_memo_search():
    """Тест поиска по памятке."""
    logger.info("\n" + "=" * 60)
    logger.info("ТЕСТ 3: Поиск по памятке")
    logger.info("=" * 60)

    try:
        from src.infrastructure.opensearch import search

        idx_name = os.getenv("MEMO_IDX_NAME", "upsrb_mortgage_memo_20251028")
        logger.info(f"Индекс памятки: {idx_name}")

        # Текстовый поиск
        results = search.text_search(
            search_queries=["ипотека документы"],
            idx_name=idx_name,
            search_fields=["text", "title"],
            top_n=3,
        )

        logger.info(f"Текстовый поиск: найдено {len(results)} результатов")

        # Векторный поиск (нужны embeddings)
        try:
            from langchain_gigachat import GigaChatEmbeddings

            embeddings = GigaChatEmbeddings(
                model="EmbeddingsGigaR",
                credentials=os.environ["GIGACHAT_SECRET"],
                scope="GIGACHAT_API_CORP",
                verify_ssl_certs=False,
            )

            vectors = embeddings.embed_documents(["ипотека документы"])
            vector_results = search.vector_search(
                vectors=vectors,
                idx_name=idx_name,
                size=3,
            )

            logger.info(f"Векторный поиск: найдено {len(vector_results)} результатов")

        except Exception as e:
            logger.warning(f"Векторный поиск не удался: {e}")

        return True

    except Exception as e:
        logger.error(f"Ошибка при поиске по памятке: {e}")
        return False


def test_graph():
    """Тест полного графа."""
    logger.info("\n" + "=" * 60)
    logger.info("ТЕСТ 4: Полный граф пайплайна")
    logger.info("=" * 60)

    try:
        from src.graph.memo_kplus_graph import build_graph

        graph = build_graph()
        logger.info("Граф собран")

        test_query = "Какие документы нужны для ипотеки?"
        logger.info(f"Тестовый запрос: {test_query}")

        response = graph.invoke({
            "messages": [("user", test_query)]
        })

        logger.info("Ответ получен:")
        final_response = response.get("final_response")
        if final_response:
            logger.info(f"  - Памятка: {'есть' if final_response.memo.content else 'нет'}")
            logger.info(f"  - Консультант+: {'есть' if final_response.k_plus.content else 'нет'}")

        return True

    except Exception as e:
        logger.error(f"Ошибка при тестировании графа: {e}")
        return False


def main():
    """Запустить все тесты."""
    tests = [
        ("API клиент К+", test_kplus_client),
        ("OpenSearch клиент", test_opensearch_client),
        ("Поиск по памятке", test_memo_search),
        ("Полный граф", test_graph),
    ]

    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            logger.error(f"Критическая ошибка в тесте '{name}': {e}")
            results[name] = False

    # Итоги
    logger.info("\n" + "=" * 60)
    logger.info("ИТОГИ ТЕСТИРОВАНИЯ")
    logger.info("=" * 60)

    for name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status}: {name}")

    total_passed = sum(results.values())
    logger.info(f"\nПройдено: {total_passed}/{len(results)}")


if __name__ == "__main__":
    main()
