FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY app/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

RUN useradd --system --uid 10001 --no-create-home appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv
COPY app/ ./app/

USER appuser
EXPOSE 8000

ENV TIMEOUT_GRACEFUL_SHUTDOWN=20
STOPSIGNAL SIGTERM
CMD ["python", "-m", "app.serve"]
