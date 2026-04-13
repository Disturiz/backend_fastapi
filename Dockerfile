FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias del sistema necesarias para JPype/JDBC
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre \
    gcc \
    g++ \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias primero para aprovechar caché
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copiar proyecto
COPY . /app

# Exponer puerto FastAPI
EXPOSE 8000

# Ajuste sugerido: dentro del contenedor el jar queda en /app/drivers/jt400.jar
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
