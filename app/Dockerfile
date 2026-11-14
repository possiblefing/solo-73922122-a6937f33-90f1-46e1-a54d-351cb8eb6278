FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Build context is the repository root (see docker-compose.yml) so the
# bundled sample CSVs can be shipped inside the image at /samples.
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/
COPY samples/ /samples/

EXPOSE 8000

# Single worker: the whole site runs exactly one replay round at a time and
# shares one two-connection pool in one process.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
