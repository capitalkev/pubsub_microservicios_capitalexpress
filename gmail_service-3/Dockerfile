# gmail_service-3/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY utils.py .
COPY token.json .
COPY credentials.json . 
COPY operaciones-peru-7e9aa471252f.json .

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]