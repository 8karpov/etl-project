# ETL: Weekly Aggregation of Trades
![ETL Workflow](https://github.com/8karpov/etl-project/actions/workflows/main.yml/badge.svg)

Автоматизированный ETL-проект: загрузка, преобразование и визуализация данных с CI/CD через GitHub Actions.
## Описание
Проект демонстрирует простой ETL-процесс:
- загрузка данных о сделках (CSV);
- очистка и нормализация данных;
- агрегация по week_start_date, client_type, user_id, symbol;
- расчёт total_volume, avg_price, trade_count, total_pnl;
- сохранение базы в agg_result.db , а также экспорт отчёта (топ-3 bronze-клиентов с наибольшим total_volume и total_pnl) и графиков.

## Структура папок
```plaintext
etl-project/
├── .github/
│ └── workflows/
│  └── etl.yml # CI/CD пайплайн (GitHub Actions)
├── data/
│ └── trades.csv # Исходные данные
├── output/
│ ├── agg_result.db # База с итогами агрегации
│ ├── top_clients.csv # Топ-3 bronze-клиентов с наибольшим total_volume и total_pnl
│ ├── weekly_volume.png # График объёмов по неделям
│ └── symbols_total_volume.png # График торгового объема каждого актива, который есть в БД
├─ scripts/
│  └── etl.py             # рабочий пайплайн
├── requirements.txt
├─ notebooks/
│  └── notebook.ipynb     # мой черновик, который послужил стартом работы над проектом и базой для создания файла `etl.py`.
├─ README.md
└─ .gitignore
```

# 1. Запуск ETL вручную
## I. Установка зависимостей/Требования
Установить:
```plaintext
pip install -r requirements.txt
```

Минимальный список требований:
pandas
matplotlib
openpyxl

## ІІ. Запуск скрипта
```plaintext
python scripts/etl.py \
  --data data/trades.csv \
  --db output/agg_result.db \
  --table agg_trades_weekly \
  --top output/top_clients \
  --chart-weekly output/weekly_volume.png \
  --chart-symbols output/symbols_total_volume.png \
  --null-timestamp drop
```
## Аргументы
- `--data` — путь к исходному CSV.
- `--db` — база с итогами агрегации.
- `--table` — имя таблицы в базе.
- `--top` — путь к отчёту топ-3 bronze-клиентов с наибольшим total_volume и total_pnl.
- `--chart-weekly` — путь к графику динамики объёмов по неделям.
- `--chart-symbols` — путь к графику распределения объёмов по symbols.
- `--no-pnl` — отключить расчёт PnL.
- `--null-timestamp` — как обрабатывать пустые даты (`drop`, `fill`, `error`).
## Результаты
- `agg_result.db` — база SQLite (таблица `agg_trades_weekly`).
- `top_clients.csv` и `.xlsx` — Топ-3 bronze-клиентов с наибольшим total_volume и total_pnl.
- `weekly_volume.png` — график динамики объёмов.
- `symbols_total_volume.png` — график торгового объема каждого актива, который есть в БД.

# 2. Как работает CI/CD
  CI/CD реализован через GitHub Actions в файле `etl.yml`, который находиться в папке `.github/workflows/`
## Триггеры:
- on: push (при каждом пуше) и workflow_dispatch (ручной запуск).
## Шаги пайплайна:
- Checkout кода
- Установка Python + `requirements.txt`
- Запуск `etl.py` с указанными аргументами
- Сохранение артефактов в output/ (db/csv/png) и публикация их как Artifacts
## Возможности развития:
- Деплой/передача данных в хранилище/warehouse
- Создание запуска по расписанию

# 3. Как бы адаптировал решение под 100+ млн строк
## Хранилище и формат данных
- CSV → Parquet/Delta (колоночный формат, сжатие, predicate pushdown).
- Локальные файлы → объектное хранилище (S3/GCS/Azure Blob) с партиционированием по дате/неделе (year=YYYY/month=MM/week=WW).
- Слоистая модель: bronze (raw) → silver (cleansed) → gold (aggregated). 
```plain text
не путать с типом клиентов: которые есть у нас в данных (client_type: gold, silver, bronze)
```
## Вычислительный движок
- Pandas → Polars (lazy) или Dask для десятков миллионов строк.
- Дальше — Apache Spark / Databricks для распределённой обработки (batch/stream).

## Оркестрация и расписания
- От разовой GitHub Actions → к Apache Airflow (или Prefect) с DAG’ами, ретраями, SLA и инкрементальными джобами (watermark).
## Валидация качества данных
- Добавить Great Expectations или Pandera: схемы, правила (типы, NULL rate, диапазоны), отчёты по quality-gates на каждом слое.
## Сервинг и аналитика
- SQLite → DWH: BigQuery / Snowflake / Redshift / ClickHouse.
- Tableau подключается к gold-слою/DWH: дашборды “Weekly Volume”, “Top Symbols”, “PnL”, автообновление экстрактов по расписанию.
## Мониторинг и алертинг
- Экспорт метрик в Prometheus, дашборды и алерты в Grafana.
- Метрики: длительность задач, throughput, error-rate, freshness, %NULL, отбраковка строк, P95/P99 latency, SLA-бриджи.
## Инфраструктура и доставка
- Контейнеризация Docker, оркестрация Kubernetes (или managed Spark).
- Хранение артефактов/логов: Object Storage + централизованный логинг (ELK / OpenSearch).
## Мини-план миграции (пошагово)
``` plain text
Шаг 1: CSV → Parquet + партиционирование в S3/GCS; Pandas → Polars (lazy).
Шаг 2: Ввести слои bronze/silver/gold и правила валидации (Great Expectations).
Шаг 3: Перенести агрегаты в DWH (BigQuery/Snowflake/Redshift/ClickHouse).
Шаг 4: Подключить Tableau к DWH (gold), настроить обновление экстрактов.
Шаг 5: Оркестрация в Airflow (DAG, ретраи, инкрементальные загрузки).
Шаг 6: Метрики в Prometheus, мониторинг/алерты в Grafana.
```

# 4. Какие технологии заменю/добавлю
- Формат данных: CSV → Parquet/Delta (колоночный, сжатие, predicate pushdown)
- Хранилище файлов (Data Lake): локально → S3/GCS/Azure Blob
- Вычисления: Pandas → Polars (lazy) / Dask → при дальнейшем росте Apache Spark (или Databricks)
- DWH/Serving-слой: SQLite → BigQuery / Snowflake / Redshift / ClickHouse
- Оркестрация: GitHub Actions (только CI) → Apache Airflow (или Prefect) для DAG’ов, ретраев, зависимостей
- Валидация данных: добавить Great Expectations / Pandera
- Визуализация: встроенные графики → Tableau (дашборды для бизнеса)
- Мониторинг: логи → Grafana (+ Prometheus метрики, алерты)

# 5. Какую архитектуру ETL предлагаю
Модель Bronze → Silver → Gold + оркестратор (Airflow):
## I. Bronze (Raw / Ingestion)
- Приём сырых данных (файлы/API/стрим) → S3 в формате Parquet/Delta
- Партиционирование по дате (year=YYYY/month=MM/week=WW)
- Каталогизация (Glue/Unity Catalog — по ситуации)
## II. Silver (Cleansed / Normalized)
- Очистка, приведение типов, дедупликация, нормализация timestamp/client_type/symbol/side
- Выгрузка в S3 (Parquet/Delta) c теми же партициями
## III. Gold (Aggregations / Serving)
- Группировки по week_start_date, client_type, user_id, symbol
- Метрики: total_volume, trade_count, avg_price, total_pnl
- Загрузка в DWH (BigQuery/Snowflake/Redshift/ClickHouse) для BI/аналитики
- Tableau подключается к Gold-слою (или напрямую к DWH)
## IV. Оркестрация и расписания
- Airflow DAG: задачи Bronze→Silver→Gold, ретраи, SLA, эмиссия метрик
- Инкрементальная обработка: только новые/изменённые партиции (watermark)
## V. Доставка отчётов
- Для статических выгрузок — S3/Share + ссылки
- Для дашбордов — Tableau Server/Cloud (освежение экстрактов по расписанию)

# 6. Метрики мониторинга ETL
Собирать метрики (Prometheus экспортер / Airflow → StatsD/Prometheus) и визуализировать в Grafana:
## I. Качество данных:
- % null values по ключевым колонкам (timestamp, user_id, symbol, price, quantity)
- % некорректных типов / парсинг-ошибок
- % поздних событий (late data) и лаг опоздания, (freshness: max(event_time) vs now).
- Доля отброшенных строк на каждом шаге
- Резкие падения/скачки total_volume, trade_count, total_pnl
- Отсутствие данных по ожидаемым партициям/клиентам/символам
## II. Производительность:
- Длительность выполнения задач (end-to-end и по шагам), P50/P95/P99
- Throughput (строк/сек, байт/сек)
- Ресурсы (CPU/Memory) — верхние границы и пики
## III. Надёжность:
- Error rate по шагам
- Повторные попытки (retries), число фейлов
- SLA-нарушения, пропуски расписания
- Cтатус DAG’ов/тасок (успех/фейл)
  
```plaintext
в Grafana: триггеры на пороги (ошибки, null values rate, freshness), доставку в Slack/Email.
```

# 7. Где будут храниться входные и выходные данные
## I. Локально (MVP):
- Вход: data/*.csv
- Выход: output/*.db, output/*.csv, output/*.png
## II. В прод-варианте:
- Data Lake: S3/GCS/Azure Blob (формат Parquet/Delta, партиционирование)
- DWH/Serving: BigQuery / Snowflake / Redshift / ClickHouse (Gold-слой, источники для Tableau)
- Каталоги/метаданные: (по необходимости) AWS Glue / Unity Catalog / Data Catalog
- Артефакты отчётов: S3/Share + дашборды в Tableau (обновляются по расписанию) 
