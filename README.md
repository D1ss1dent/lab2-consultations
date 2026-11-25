# Лабораторна робота 2. Метрики сервісу і межа масштабованості, варіант 14

Сервіс консультативних висновків `/consultations` з ЛР1 з метриками Prometheus, коректною зупинкою і сервером Prometheus у Docker Compose. Python 3.12, FastAPI, PostgreSQL 16, nginx, Prometheus 2.55.

Звіт лежить у файлі [REPORT.md](REPORT.md).

## Запуск

```bash
cp .env.example .env
docker compose up -d --build
```

На Windows замість `cp` можна використати `Copy-Item .env.example .env`. У файлі `.env` треба вказати свій пароль до бази.

Сервіс доступний на http://localhost:8080, Prometheus на http://localhost:9090.

Запуск з чотирма вебінстансами:

```bash
docker compose up -d --scale web=4
docker compose restart lb
```

`restart lb` потрібен після кожної зміни кількості інстансів, бо nginx бере адреси реплік при старті.

## Що де лежить

- `app/` - код сервісу, `app/serve.py` - запуск uvicorn з коректною зупинкою
- `prometheus/prometheus.yml` - конфіг збору метрик
- `nginx/nginx.conf` - конфіг балансувальника
- `loadgen/loadgen.py` - генератор навантаження
- `scripts/` - прогони, обробка результатів, заповнення бази, перевірка CRUD
- `results/lab2/` - результати прогонів
- `docs/lab2/` - діаграми і скриншоти

## Скрипти

```bash
pip install -r requirements-tools.txt
python scripts/lab2.py info
python scripts/lab2.py up --n 4 --build
python scripts/lab2.py check
python scripts/lab2.py graceful
python scripts/lab2.py noop
python scripts/lab2.py run --n 1
python scripts/lab2.py run --n 2
python scripts/lab2.py run --n 3
python scripts/lab2.py run --n 4
python scripts/analyze_lab2.py
```

Генератор використовує тільки стандартну бібліотеку Python, matplotlib потрібен для діаграм.
