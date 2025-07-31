# Usa la imagen completa de Python 3.9
FROM python:3.9

WORKDIR /app

# Copia los requerimientos y el nuevo archivo de credenciales
COPY requirements.txt .
COPY service_account.json .
COPY main.py .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]