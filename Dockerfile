FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# zoneinfo("Europe/Warsaw") needs tzdata on slim images
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend

RUN mkdir -p /app/data

EXPOSE 8000

ENV PLAN_DB_PATH=/app/data/plan.sqlite3

CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
