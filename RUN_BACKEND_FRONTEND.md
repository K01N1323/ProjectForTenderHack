# Run Backend + Frontend

## 1. Подготовить Python-зависимости

```bash
python3 -m pip install --user -r requirements-backend.txt
python3 -m pip install --user -r requirements-semantic.txt
```

## 2. Убедиться, что собраны данные

Если `data/processed/` уже есть, этот шаг можно пропустить.

```bash
python3 scripts/preprocess_data.py
python3 scripts/build_search_assets.py
python3 scripts/build_offer_assets.py
python3 scripts/train_fasttext.py
```

## 3. Запустить backend

Из корня проекта:

```bash
python3 -m uvicorn backend.main:app --reload
```

Проверка:

```bash
curl http://127.0.0.1:8000/api/health
```

## 4. Запустить frontend

Из папки [frontend](/Users/nosovdanil/PycharmProjects/ProjectForTenderHack/frontend):

```bash
npm install
npm run dev
```

По умолчанию фронт ходит в:

```text
http://127.0.0.1:8000
```

Если backend на другом адресе, задайте:

```bash
VITE_API_BASE_URL=http://127.0.0.1:8000 npm run dev
```

## 5. Что уже работает

- `POST /api/auth/login`
- `POST /api/search`
- `GET /api/search/suggestions`
- поиск по СТЕ с typo correction, synonyms, semantic layer и personalization
- фронт уже подключён к живому backend через `axios`

## 6. Быстрый smoke test

Вход:

- ИНН: `7714338609`

Проверить запросы:

- `парацетомол 500 мг`
- `флешка`
- `мфу`
- `канцелярские ручки`

## 7. Что важно

- backend берёт цену и `supplierInn` из `ste_offer_lookup`
- если таблицы `ste_offer_lookup` нет, поиск всё равно работает, но цена будет оценочной
- для лучшего semantic rerank желательно иметь файл `data/processed/tenderhack_fasttext.bin`
