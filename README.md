# ProjectForTenderHack

Прототип персонализированного поиска СТЕ для Tender Hack и Портала поставщиков.

Проект решает задачу поиска по каталогу СТЕ с учетом:

- опечаток в запросе
- синонимов и словоформ
- семантической близости
- истории закупок заказчика
- поведения пользователя в текущей сессии
- объяснимого ранжирования результатов

В репозитории есть рабочий backend на `FastAPI`, frontend на `React + Vite`, офлайн-пайплайн подготовки данных и артефакты для персонализации.

## Что уже умеет система

- искать СТЕ по названию, категории и ключевым токенам
- исправлять типовые опечатки
- расширять запрос синонимами
- использовать семантический слой для сокращений и близких формулировок
- персонализировать выдачу по ИНН заказчика, региону и истории закупок
- учитывать онлайн-сигналы из текущей сессии
- понижать категории после быстрого отказа
- возвращать причины, почему товар показан выше
- отдавать search suggestions и corrected query

## Архитектура

Система состоит из четырех основных слоев.

### 1. Data pipeline

Скрипты из `scripts/` подготавливают данные и поисковые артефакты:

- `scripts/preprocess_data.py`
- `scripts/build_search_assets.py`
- `scripts/build_offer_assets.py`
- `scripts/train_fasttext.py`
- `scripts/run_personalization_pipeline.py`

На выходе формируются:

- очищенные и агрегированные таблицы в `data/processed/`
- SQLite-базы для поиска и персонализации
- справочники и артефакты в `artifacts/`
- офлайн-отчеты в `reports/`

### 2. Search layer

Основной поиск реализован в `src/tenderhack/search.py`.

Pipeline запроса:

1. нормализация текста
2. typo correction
3. расширение синонимами
4. семантическое расширение
5. retrieval через SQLite FTS / BM25
6. дополнительный lexical + semantic scoring

### 3. Personalization layer

Персонализация реализована в:

- `src/tenderhack/personalization.py`
- `src/tenderhack/personalization_runtime.py`
- `src/tenderhack/personalization_model.py`
- `src/training/`

Используемые сигналы:

- история закупок конкретного заказчика
- любимые категории и ранее купленные СТЕ
- региональные предпочтения
- популярность у похожих заказчиков
- действия в текущей сессии: открытие карточки, клик 

### 4. Product layer

- backend: `backend/main.py`
- frontend: `frontend/`

Frontend работает с живым API и умеет отправлять пользовательские события обратно в backend, чтобы демонстрировать динамическое изменение выдачи.

## Структура репозитория

```text
backend/                 FastAPI API
frontend/                React + Vite клиент
scripts/                 подготовка данных и сборка артефактов
src/tenderhack/          поиск, персонализация, online state, cache
src/training/            offline personalization pipeline
data/reference/          синонимы и тестовые запросы
artifacts/               feature spec, explain rules, offline metrics
reports/                 data contract и offline evaluation
tests/                   unit и API тесты
```

## Технологии

- Python
- FastAPI
- SQLite
- CatBoost
- fastText
- Redis или in-memory cache
- React
- TypeScript
- Zustand
- Vite

## Быстрый запуск

### 1. Установить backend-зависимости

```bash
python3 -m pip install --user -r requirements-backend.txt
python3 -m pip install --user -r requirements-semantic.txt
python3 -m pip install --user -r requirements-personalization.txt
```

Примечание:

- для `catboost` рекомендуется `Python 3.11` или `3.12`
- в `requirements-personalization.txt` CatBoost помечен как optional runtime dependency для финального ranker

### 2. Подготовить данные

Если `data/processed/` уже собрана, шаг можно пропустить.

```bash
python3 scripts/preprocess_data.py
python3 scripts/build_search_assets.py
python3 scripts/build_offer_assets.py
python3 scripts/train_fasttext.py
```

Для офлайн-персонализации:

```bash
python3 scripts/run_personalization_pipeline.py
```

### 3. Запустить backend

```bash
python3 -m uvicorn backend.main:app --reload
```

Проверка:

```bash
curl http://127.0.0.1:8000/api/health
```

### 4. Запустить frontend

```bash
cd frontend
npm install
npm run dev
```

По умолчанию frontend ходит в:

```text
http://127.0.0.1:8000
```

Если backend поднят на другом адресе:

```bash
VITE_API_BASE_URL=http://127.0.0.1:8000 npm run dev
```

## API

Сейчас в backend доступны:

- `GET /api/health`
- `POST /api/auth/login`
- `POST /api/search`
- `GET /api/search/suggestions`
- `POST /api/event`

### Что возвращает поиск

Поиск отдает:

- список товаров
- общее количество кандидатов
- `correctedQuery`, если запрос был исправлен
- персонализированные причины показа через `reasonToShow`

## Демо-сценарии

Для быстрой демонстрации удобно использовать:

- ИНН: `7714338609`
- запрос `парацетомол 500 мг`
- запрос `флешка`
- запрос `мфу`
- запрос `канцелярские ручки`

Хороший live demo выглядит так:

1. пользователь логинится по ИНН
2. вводит запрос с опечаткой или синонимом
3. получает исправленную и персонализированную выдачу
4. открывает карточку или добавляет товар в корзину
5. повторно ищет и видит, что ранжирование изменилось

## Тестирование

В репозитории есть unit- и API-тесты:

- `tests/test_search.py`
- `tests/test_personalization.py`
- `tests/test_api.py`

Запуск:

```bash
python3 -m unittest tests.test_search tests.test_personalization tests.test_api
```

## Офлайн-персонализация

В проекте есть отдельный офлайн-контур ранжирования:

- data contract: `reports/data_contract.md`
- offline evaluation: `reports/offline_eval.md`
- feature spec: `artifacts/feature_spec.json`
- explain rules: `artifacts/explain_rules.json`
- defaults: `artifacts/feature_defaults.json`
- training config: `artifacts/train_config.yaml`

Stable inference entrypoint:

```python
predict_personalization(candidates, user_profile, query_features)
```

## Ограничения текущего состояния

- исходные большие датасеты не закоммичены в репозиторий
- без реальных входных CSV офлайн-оценка в `reports/offline_eval.md` остается в статусе `missing_input`
- если таблица `ste_offer_lookup` не собрана, поиск все равно работает, но цена и поставщик могут быть оценочными
- часть качества демонстрации зависит от наличия подготовленных `data/processed/` артефактов

## Текущее позиционирование проекта

Текущий MVP показывает:

- интеллектуальный поиск по СТЕ
- динамическую персонализацию
- объяснимость ранжирования
- рабочую связку backend + frontend
- основу для дальнейшего обучения ML-ранкера на офлайн-истории закупок
