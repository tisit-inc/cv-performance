FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system --no-cache .

FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY app/ app/

ARG GIT_COMMIT=hash
ENV GIT_COMMIT=$GIT_COMMIT

ARG BUILD_TIME_MOSCOW=hash
ENV BUILD_TIME_MOSCOW=$BUILD_TIME_MOSCOW

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8000


CMD ["python", "app/main.py"]
