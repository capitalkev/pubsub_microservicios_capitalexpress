import os
import io
import base64
import json
import traceback
import mimetypes # Importar mimetypes
from fastapi import FastAPI, HTTPException, Request, Response, status, BackgroundTasks
from google.cloud import storage, pubsub_v1
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from typing import Optional

app = FastAPI(title="Drive Service (Optimizado)")

# --- Configuración ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID", "1dl5FE6wKk6aXfspFrjm5YUs9rHP92Q_5")
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
TOPIC_FILES_ARCHIVED = publisher.topic_path(GCP_PROJECT_ID, "files-archived")

try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
except Exception as e:
    drive_service = None
    print(f"ADVERTENCIA: No se pudo inicializar el servicio de Drive. Error: {e}")

def upload_files_in_background(all_gcs_paths: list, folder_id: str, tracking_id: str):
    print(f"DRIVE-BG: Iniciando subida en segundo plano para {tracking_id}")
    for gcs_path in all_gcs_paths:
        try:
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            file_bytes = blob.download_as_bytes()
            mime_type, _ = mimetypes.guess_type(os.path.basename(gcs_path))
            mime_type = mime_type or 'application/octet-stream'

            file_metadata = {'name': os.path.basename(gcs_path), 'parents': [folder_id]}
            media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=True)
            drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
        except Exception as e:
            print(f"WARN-BG: Falló la subida de '{gcs_path}'. Error: {e}")
    print(f"DRIVE-BG: Subida en segundo plano para {tracking_id} completada.")

@app.post("/pubsub-handler", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_handler(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    if not body or "message" not in body:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido.")

    try:
        message_data = base64.b64decode(body["message"]["data"]).decode("utf-8")
        payload = json.loads(message_data)
        tracking_id = payload["tracking_id"]

        # ETAPA RÁPIDA: Crear carpeta y publicar el link
        operation_id = payload.get("operation_id", tracking_id)
        folder_name = f"Operacion_{operation_id}"
        folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [DRIVE_PARENT_FOLDER_ID]}
        folder = drive_service.files().create(body=folder_metadata, fields='id, webViewLink', supportsAllDrives=True).execute()
        folder_id = folder.get('id')
        folder_url = folder.get('webViewLink')

        print(f"DRIVE: Carpeta '{folder_name}' creada. Publicando link.")
        
        # Publicar el mensaje con el link inmediatamente
        payload["drive_folder_url"] = folder_url
        next_message_data = json.dumps(payload).encode("utf-8")
        publisher.publish(TOPIC_FILES_ARCHIVED, next_message_data).result()
        
        # ETAPA LENTA: Delegar la subida de archivos a un segundo plano
        gcs_paths = payload.get("gcs_paths", {})
        all_gcs_paths = gcs_paths.get('xml', []) + gcs_paths.get('pdf', []) + gcs_paths.get('respaldo', [])
        background_tasks.add_task(upload_files_in_background, all_gcs_paths, folder_id, tracking_id)

    except Exception as e:
        traceback.print_exc()
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    return Response(status_code=status.HTTP_204_NO_CONTENT)