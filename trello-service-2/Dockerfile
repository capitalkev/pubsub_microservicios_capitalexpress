# Usa una imagen base oficial de Python.
FROM python:3.9-slim

# Establece la variable de entorno para que Python no almacene en búfer la salida,
# lo que permite ver los logs de la aplicación en tiempo real.
ENV PYTHONUNBUFFERED True

# Establece el directorio de trabajo dentro del contenedor.
WORKDIR /app

# Copia el archivo de dependencias y las instala.
# Copiarlo por separado aprovecha el caché de Docker.
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de tu aplicación al directorio de trabajo.
COPY . .

# Expone el puerto 8080. Cloud Run enviará las solicitudes a este puerto.
EXPOSE 8080

# Comando para iniciar el servidor de producción Uvicorn.
# Escucha en todas las interfaces de red (0.0.0.0) en el puerto 8080.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]