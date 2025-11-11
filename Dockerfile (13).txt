# ======== BASE IMAGE ========
FROM python:3.11-slim

# ======== WORKDIR ========
WORKDIR /app

# ======== ENVIRONMENT CONFIG ========
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# ======== SYSTEM DEPENDENCIES ========
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# ======== INSTALL DEPENDENCIES ========
COPY requirements.txt .

# обновляем pip и колёса, очищаем кеш и ставим зависимости
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# ======== COPY PROJECT FILES ========
COPY . .

# ======== RUN APP ========
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
