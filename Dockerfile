# sync-tables: every role of the table sync (source, sink, serving source, relay), in one image,
# with the `events` extra (celine-sdk: the sink's and the local relay's MQTT publisher).
# The image carries code only. Connection URLs, the certificates, the token, the
# allowlists and any profile arrive at run time.
FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8.13 /uv /bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra events --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --extra events --no-editable

FROM python:3.13-slim
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001
ENTRYPOINT ["sync-tables"]
CMD ["--help"]
