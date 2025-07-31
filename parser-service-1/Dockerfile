# Usa una imagen base oficial de Python.
FROM python:3.9-slim

# Establece variables de entorno para un logging eficiente.
ENV PYTHONUNBUFFERED=True

# Establece el directorio de trabajo dentro del contenedor.
WORKDIR /app

# Copia el archivo de dependencias y las instala.
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código de tu aplicación al directorio de trabajo.
COPY . .

# Expone el puerto 8080. Cloud Run enviará las solicitudes a este puerto.
EXPOSE 8080

# Comando para iniciar el servidor de producción Uvicorn.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]