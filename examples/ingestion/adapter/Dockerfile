FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY ingest/ ingest/
RUN pip install --no-cache-dir ".[demo]"

EXPOSE 8090
HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=30 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8090/healthz')"

CMD ["python", "-m", "ingest.server"]
