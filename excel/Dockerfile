# excel/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expone el nuevo puerto
EXPOSE 8080

# Ejecuta la aplicación en el puerto 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]