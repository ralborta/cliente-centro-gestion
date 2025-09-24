FROM public.ecr.aws/docker/library/python:3.11-slim

WORKDIR /app

# Dependencias del sistema para pdfplumber (poppler-utils no es obligatorio aquí)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]


