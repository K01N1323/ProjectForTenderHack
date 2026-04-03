# Что уже сделано и что брать дальше

## Что уже готово

В проекте уже сделаны базовые вещи для двух критичных треков: `search` и `personalization`.

### 1. Подготовка данных

Сделан preprocessing сырых данных:

1. Проверена структура CSV.
2. Очищены и нормализованы названия СТЕ.
3. Выделены:
   - `clean_name`
   - `normalized_name`
   - `category`
   - `normalized_category`
   - `attribute_keys`
   - `key_tokens`
4. Построены агрегаты:
   - `customer -> ste`
   - `customer -> category`
   - `region -> category`

Скрипт:

- [preprocess_data.py](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/scripts/preprocess_data.py)

### 2. Search baseline

Сделан рабочий baseline-поиск по СТЕ.

Что есть:

1. Функция `search_ste(query, top_k)`.
2. Поиск по полям:
   - `clean_name`
   - `normalized_name`
   - `category`
   - `normalized_category`
   - `key_tokens`
3. Поддержка:
   - словоформ через простой stem;
   - синонимов;
   - исправления опечаток.
4. Возврат top-N результатов с признаками ранжирования.

Код:

- [search.py](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/src/tenderhack/search.py)
- [text.py](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/src/tenderhack/text.py)

### 3. Синонимы и тестовые запросы

Подготовлены:

1. Частотный словарь синонимов.
2. Набор тестовых запросов для проверки качества.

Файлы:

- [search_synonyms.json](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/data/reference/search_synonyms.json)
- [search_test_queries.json](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/data/reference/search_test_queries.json)

### 4. Personalization

Сделан рабочий модуль персонализации.

Что есть:

1. `build_customer_profile(customer_inn, customer_region=None)`
2. `rerank_ste(results, customer_profile, session_state)`
3. `rerank_offers(offers, customer_profile, session_state)`
4. Объяснения ранжирования.

Какие сигналы используются:

1. Часто закупаемые СТЕ заказчика.
2. Часто закупаемые категории заказчика.
3. Региональные предпочтения.
4. Сессионные действия:
   - click
   - cart
   - recent category

Код:

- [personalization.py](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/src/tenderhack/personalization.py)

### 5. Builder для поисковых артефактов

Сделан отдельный скрипт сборки поисковых и персонализационных данных.

Что он делает:

1. Строит dedup search DB по СТЕ.
2. Строит частотный словарь токенов для typo-correction.
3. Строит `customer_region_lookup`.

Скрипт:

- [build_search_assets.py](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/scripts/build_search_assets.py)

### 6. Документы по постановке и плану

Уже есть:

- [HACKATHON_ACTION_PLAN.md](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/HACKATHON_ACTION_PLAN.md)
- [CUSTOMER_INSTRUCTION_5P_PLAN.md](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/CUSTOMER_INSTRUCTION_5P_PLAN.md)

## Что уже проверено

Ручная проверка уже показала:

1. `парацетомол 500 мг` исправляется в `парацетамол 500 мг`.
2. `трамодол 100 мг` исправляется в `трамадол 100 мг`.
3. `флеш накопитиль 16 гб` исправляется в `флеш накопитель 16 гб`.
4. `канцелярские ручки` корректно выводит `Ручки канцелярские`.
5. Персонализация по заказчику `7714338609` поднимает релевантные категории `ИММУНОДЕПРЕССАНТЫ,L04`.

## Что не сделано

Пока не сделано:

1. Backend API.
2. UI.
3. Реальная карточка СТЕ в приложении.
4. Реальные оферты как отдельный слой данных.
5. Корзина и flow закупки в коде приложения.

## Что брать следующим участникам

### Backend engineer

Нужно сделать:

1. `GET /search/ste?q=...`
2. `GET /ste/{ste_id}`
3. `GET /ste/{ste_id}/offers`
4. `POST /cart/add`
5. `GET /cart`
6. `POST /cart/create-procurement`
7. `POST /session/event`

На что опираться:

1. `search.SearchService`
2. `personalization.PersonalizationService`

### Frontend engineer

Нужно собрать минимум 4 экрана:

1. Поиск СТЕ.
2. Карточка СТЕ.
3. Корзина.
4. Выбор типа закупки.

Что уже можно брать из backend-контракта:

1. Результат поиска должен показывать:
   - `ste_id`
   - `clean_name`
   - `category`
   - `search_score`
   - `explanation`
2. Для демо надо показывать:
   - исправленный запрос;
   - причины ранжирования;
   - изменение выдачи после действия пользователя.

### Product/Analyst

Нужно добить:

1. BPMN по заказческому пути:
   - поиск СТЕ
   - карточка СТЕ
   - оферта
   - корзина
   - тип закупки
   - прямая закупка
2. Ручной benchmark на `10-20` запросах.
3. Demo script на 3 кейса:
   - typo
   - synonym/wordform
   - personalization after action

## Как локально пересобрать данные

### Шаг 1. preprocessing

```bash
python3 scripts/preprocess_data.py
```

### Шаг 2. search assets

```bash
python3 scripts/build_search_assets.py
```

### Шаг 3. быстрый smoke test

```bash
PYTHONPATH=src python3 - <<'PY'
from tenderhack.search import SearchService
svc = SearchService()
print(svc.search("парацетомол 500 мг", top_k=3))
svc.close()
PY
```

## Главный смысл текущего состояния

Сейчас у команды уже есть не просто сырые данные, а рабочая база для следующего слоя.

То есть:

1. Поиск можно сразу встраивать в API.
2. Персонализацию можно сразу встраивать в rerank.
3. Фронтенд можно собирать уже под живые структуры ответа.
4. Следующий фокус команды должен быть на `API + UI + корзина + сценарий закупки`, а не на повторной подготовке данных.
