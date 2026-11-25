import os
import socket
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import date
from typing import Optional

import psycopg
from fastapi import FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import (CONTENT_TYPE_LATEST, REGISTRY, Counter, Gauge,
                               Histogram, disable_created_metrics, generate_latest)
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.routing import Match

POD_NAME = os.getenv("POD_NAME") or socket.gethostname()
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "15"))

DEFAULT_LIMIT = 20
MAX_LIMIT = 100
MIN_CONCLUSION_LEN = 20
MAX_ID = 9223372036854775807

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
POOL_WAIT_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
                     0.25, 0.5, 1.0, 2.5, 5.0)

disable_created_metrics()

HTTP_REQUESTS = Counter(
    "http_requests_total", "Кількість оброблених HTTP-запитів",
    ["method", "path", "code"])
HTTP_DURATION = Histogram(
    "http_request_duration_seconds", "Тривалість обробки HTTP-запиту в сервісі, с",
    ["method", "path"], buckets=LATENCY_BUCKETS)
HTTP_IN_PROGRESS = Gauge(
    "http_requests_in_progress", "Кількість запитів, що зараз обробляються")

DB_POOL_BUSY = Gauge(
    "db_pool_connections_busy", "Кількість зайнятих з'єднань пулу (М1)")
DB_POOL_WAITING = Gauge(
    "db_pool_requests_waiting", "Кількість потоків, що чекають на вільне з'єднання пулу")
DB_POOL_WAIT = Histogram(
    "db_pool_wait_seconds", "Час очікування з'єднання з пулу, с (М1)",
    buckets=POOL_WAIT_BUCKETS)
DB_POOL_MAX = Gauge("db_pool_size", "Розмір пулу з'єднань (DB_POOL_SIZE)")
DB_POOL_MAX.set(DB_POOL_SIZE)
APP_INFO = Gauge("app_instance_info", "Ім'я екземпляра сервісу", ["pod"])
APP_INFO.labels(pod=POD_NAME).set(1)

CONNINFO = make_conninfo(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "consultations"),
    user=os.getenv("DB_USER", "app"),
    password=os.getenv("DB_PASSWORD", ""),
    connect_timeout=3,
)

pool = ConnectionPool(
    CONNINFO,
    min_size=DB_POOL_SIZE,
    max_size=DB_POOL_SIZE,
    kwargs={"row_factory": dict_row},
    open=False,
    timeout=5,
    name="web",
)


@contextmanager
def db_conn(timeout: Optional[float] = None):
    t0 = time.perf_counter()
    DB_POOL_WAITING.inc()
    try:
        conn = pool.getconn(timeout=timeout)
    finally:
        DB_POOL_WAITING.dec()
        DB_POOL_WAIT.observe(time.perf_counter() - t0)
    DB_POOL_BUSY.inc()
    try:
        with conn:
            yield conn
    finally:
        pool.putconn(conn)
        DB_POOL_BUSY.dec()


COLS = ("id, patient_code, specialty, consultation_date, conclusion, "
        "recommendations, created_at, updated_at")


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=False)
    print(f"[startup] pod={POD_NAME} pid={os.getpid()} db_pool_size={DB_POOL_SIZE}", flush=True)
    yield
    print("[shutdown] in-flight requests finished, closing DB pool", flush=True)
    pool.close()
    print("[shutdown] DB pool closed, exiting with code 0", flush=True)


app = FastAPI(title="Consultations CRUD + metrics (lab 2, variant 14)", lifespan=lifespan)


def route_template(scope) -> str:
    route = scope.get("route")
    if route is not None and getattr(route, "path", None):
        return route.path
    for r in app.router.routes:
        match, _ = r.matches(scope)
        if match == Match.FULL:
            return getattr(r, "path", "unmatched")
    return "unmatched"


class MetricsMiddleware:
    SKIP = {"/metrics"}

    def __init__(self, asgi_app):
        self.app = asgi_app
        self.pod_header = (b"x-pod", POD_NAME.encode())

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        status = {"code": 500}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
                message["headers"] = list(message.get("headers", [])) + [self.pod_header]
            await send(message)

        if scope["path"] in self.SKIP:
            return await self.app(scope, receive, send_wrapper)

        start = time.perf_counter()
        HTTP_IN_PROGRESS.inc()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - start
            HTTP_IN_PROGRESS.dec()
            method = scope["method"]
            path = route_template(scope)
            HTTP_REQUESTS.labels(method, path, str(status["code"])).inc()
            HTTP_DURATION.labels(method, path).observe(elapsed)


app.add_middleware(MetricsMiddleware)


class ConsultationIn(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    patient_code: str = Field(min_length=1, max_length=32,
                              description="Знеособлений код пацієнта")
    specialty: str = Field(min_length=1, max_length=100,
                           description="Спеціальність консультанта")
    consultation_date: date
    conclusion: str = Field(max_length=10000, description="Текст висновку, >= 20 символів")
    recommendations: str = Field(min_length=1, max_length=10000)

    @field_validator("conclusion")
    @classmethod
    def conclusion_min_length(cls, v: str) -> str:
        if len(v) < MIN_CONCLUSION_LEN:
            raise ValueError(
                f"текст висновку має містити щонайменше {MIN_CONCLUSION_LEN} символів, отримано {len(v)}"
            )
        return v


def error(status: int, code: str, message: str, details=None) -> JSONResponse:
    body = {"error": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse(status_code=status, content=body)


@app.exception_handler(RequestValidationError)
async def validation_handler(_: Request, exc: RequestValidationError):
    details = []
    for e in exc.errors():
        loc = [str(x) for x in e.get("loc", ()) if x != "body"]
        details.append({
            "field": ".".join(loc) or "body",
            "message": str(e.get("msg", "")).removeprefix("Value error, "),
        })
    return error(400, "validation_error", "Дані запиту не пройшли валідацію", details)


@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(PoolTimeout)
async def db_unavailable_handler(_: Request, exc: Exception):
    return error(503, "db_unavailable", f"База даних недоступна: {type(exc).__name__}")


def not_found(item_id: int) -> JSONResponse:
    return error(404, "not_found", f"Консультативний висновок id={item_id} не знайдено")


IdPath = Path(alias="id", ge=1, le=MAX_ID, description="Ідентифікатор запису")


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.get("/livez")
async def livez():
    return {"status": "alive", "pod": POD_NAME}


@app.get("/healthz")
def healthz():
    try:
        with db_conn(timeout=2) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        return JSONResponse(status_code=503,
                            content={"status": "unavailable", "db": "down",
                                     "reason": type(exc).__name__, "pod": POD_NAME})
    return {"status": "ok", "db": "up", "pod": POD_NAME}


@app.post("/consultations", status_code=201)
def create_consultation(item: ConsultationIn):
    with db_conn() as conn:
        row = conn.execute(
            f"""INSERT INTO consultations
                (patient_code, specialty, consultation_date, conclusion, recommendations)
                VALUES (%s, %s, %s, %s, %s) RETURNING {COLS}""",
            (item.patient_code, item.specialty, item.consultation_date,
             item.conclusion, item.recommendations),
        ).fetchone()
    return row


@app.get("/consultations")
def list_consultations(
    specialty: Optional[str] = Query(None, min_length=1, max_length=100,
                                     description="Фільтр за спеціальністю (точний збіг)"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0, le=1_000_000_000),
):
    where, args = "", []
    if specialty is not None:
        where, args = "WHERE specialty = %s", [specialty]
    with db_conn() as conn:
        total = conn.execute(f"SELECT count(*) AS n FROM consultations {where}",
                             args).fetchone()["n"]
        items = conn.execute(
            f"SELECT {COLS} FROM consultations {where} ORDER BY id LIMIT %s OFFSET %s",
            args + [limit, offset],
        ).fetchall()
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/consultations/{id}")
def get_consultation(item_id: int = IdPath):
    with db_conn() as conn:
        row = conn.execute(f"SELECT {COLS} FROM consultations WHERE id = %s",
                           (item_id,)).fetchone()
    return row if row else not_found(item_id)


@app.put("/consultations/{id}")
def replace_consultation(item: ConsultationIn, item_id: int = IdPath):
    with db_conn() as conn:
        row = conn.execute(
            f"""UPDATE consultations
                SET patient_code = %s, specialty = %s, consultation_date = %s,
                    conclusion = %s, recommendations = %s, updated_at = now()
                WHERE id = %s RETURNING {COLS}""",
            (item.patient_code, item.specialty, item.consultation_date,
             item.conclusion, item.recommendations, item_id),
        ).fetchone()
    return row if row else not_found(item_id)


@app.delete("/consultations/{id}", status_code=204)
def delete_consultation(item_id: int = IdPath):
    with db_conn() as conn:
        cur = conn.execute("DELETE FROM consultations WHERE id = %s", (item_id,))
        deleted = cur.rowcount
    if deleted == 0:
        return not_found(item_id)
    return Response(status_code=204)
