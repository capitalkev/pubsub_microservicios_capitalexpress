import os
import requests
import time
import json
import logging
import traceback
import base64
from fastapi import FastAPI, HTTPException, Response, Request, status
from typing import List, Dict
from dotenv import load_dotenv
from google.cloud import storage, pubsub_v1

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

app = FastAPI(title="Cavali Service (Pub/Sub Enabled)")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
CAVALI_CLIENT_ID = os.getenv("CAVALI_CLIENT_ID")
CAVALI_CLIENT_SECRET = os.getenv("CAVALI_CLIENT_SECRET")
CAVALI_SCOPE = os.getenv("CAVALI_SCOPE")
CAVALI_TOKEN_URL = os.getenv("CAVALI_TOKEN_URL")
CAVALI_API_KEY = os.getenv("CAVALI_API_KEY")
CAVALI_BLOCK_URL = os.getenv("CAVALI_BLOCK_URL")
CAVALI_STATUS_URL = os.getenv("CAVALI_STATUS_URL")
GCS_BUCKET_NAME_TOKEN = os.getenv("GCS_BUCKET_NAME")
TOKEN_FILE_NAME = "cavali_token.json"

# ---- NUEVA CONSTANTE ----
# Define el tamaño del lote de XML a procesar en cada llamada a Cavali.
# Puedes ajustar este número según el tamaño promedio de tus archivos XML.
XML_BATCH_SIZE = 30

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
TOPIC_INVOICES_VALIDATED = publisher.topic_path(GCP_PROJECT_ID, "invoices-validated")

def get_cavali_token():
    """Obtiene un token de Cavali, usando un archivo en GCS como caché."""
    if not GCS_BUCKET_NAME_TOKEN:
        logging.error("La variable de entorno GCS_BUCKET_NAME para el token no está configurada.")
        raise ValueError("Configuración de GCS para token incompleta.")

    bucket = storage_client.bucket(GCS_BUCKET_NAME_TOKEN)
    blob = bucket.blob(TOKEN_FILE_NAME)

    try:
        if blob.exists():
            token_data = json.loads(blob.download_as_string())
            if token_data.get("expires_at", 0) > time.time() + 60:
                logging.info("Token válido obtenido desde GCS.")
                return token_data["access_token"]
    except Exception as e:
        logging.error(f"No se pudo leer el token desde GCS, se solicitará uno nuevo. Error: {e}")

    logging.info("Solicitando nuevo token de Cavali...")
    data = {
        "grant_type": "client_credentials", "client_id": CAVALI_CLIENT_ID,
        "client_secret": CAVALI_CLIENT_SECRET, "scope": CAVALI_SCOPE,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(CAVALI_TOKEN_URL, data=data, headers=headers, timeout=30)
    response.raise_for_status()
    new_token_data = response.json()
    
    expires_at = time.time() + new_token_data.get("expires_in", 3600)
    data_to_save = {"access_token": new_token_data["access_token"], "expires_at": expires_at}
    blob.upload_from_string(json.dumps(data_to_save), content_type="application/json")
    
    logging.info(f"Nuevo token de Cavali guardado en GCS.")
    return new_token_data["access_token"]

# ==============================================================================
# ---- FUNCIÓN `pubsub_handler` MODIFICADA ----
# ==============================================================================
@app.post("/pubsub-handler", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_handler(request: Request):
    body = await request.json()
    if not body or "message" not in body:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido.")

    try:
        message_data = base64.b64decode(body["message"]["data"]).decode("utf-8")
        payload = json.loads(message_data)
        tracking_id = payload["tracking_id"]
        xml_paths = payload.get("gcs_paths", {}).get("xml", [])
        
        logging.info(f"CAVALI: Procesando {tracking_id} con {len(xml_paths)} XMLs en lotes de {XML_BATCH_SIZE}.")

        xml_files_b64_group = []
        for gcs_path in xml_paths:
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            content_bytes = blob.download_as_bytes()
            xml_files_b64_group.append({
                "filename": os.path.basename(gcs_path),
                "content_base64": base64.b64encode(content_bytes).decode('utf-8')
            })

        token = get_cavali_token()
        headers = {"Authorization": f"Bearer {token}", "x-api-key": CAVALI_API_KEY, "Content-Type": "application/json"}
        
        # Mapa para consolidar los resultados de todos los lotes
        final_results_map = {}

        # Procesar los XML en lotes
        for i in range(0, len(xml_files_b64_group), XML_BATCH_SIZE):
            batch = xml_files_b64_group[i:i + XML_BATCH_SIZE]
            batch_filenames = [f['filename'] for f in batch]
            logging.info(f"CAVALI: Procesando lote {i//XML_BATCH_SIZE + 1} para {tracking_id} con {len(batch)} archivos: {batch_filenames}")

            invoice_xml_list = [{"name": f['filename'], "fileXml": f['content_base64']} for f in batch]
            payload_bloqueo = {"invoiceXMLDetail": {"invoiceXML": invoice_xml_list}}
            
            try:
                response_bloqueo = requests.post(CAVALI_BLOCK_URL, json=payload_bloqueo, headers=headers, timeout=300)
                response_bloqueo.raise_for_status()
                bloqueo_data = response_bloqueo.json()
                id_proceso = bloqueo_data.get("response", {}).get("idProceso")

                if not id_proceso:
                    raise ValueError(f"Cavali no retornó un idProceso para el lote. Respuesta: {bloqueo_data}")

                time.sleep(7) 
                
                payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
                response_estado = requests.post(CAVALI_STATUS_URL, json=payload_estado, headers=headers, timeout=300)
                response_estado.raise_for_status()
                cavali_response_data = response_estado.json()

                invoice_details = cavali_response_data.get("response", {}).get("Process", {}).get("ProcessInvoiceDetail", {}).get("Invoice", [])
                for invoice in invoice_details:
                    # Lógica para mapear el resultado al nombre de archivo original
                    nombre_archivo_original = "desconocido"
                    for f in batch: # Buscar solo en el lote actual
                        if (str(invoice.get("ruc", "")) in f["filename"] and
                            invoice.get("serie", "") in f["filename"] and
                            str(invoice.get("numeration", "")) in f["filename"]):
                            nombre_archivo_original = f["filename"]
                            break
                    
                    final_results_map[nombre_archivo_original] = {
                        "message": invoice.get("message"), "process_id": id_proceso, "result_code": invoice.get("resultCode")
                    }
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"Error HTTP procesando el lote para {tracking_id}. Archivos: {batch_filenames}. Error: {http_err}")
                # Marcar los archivos de este lote como fallidos
                for f in batch:
                    final_results_map[f['filename']] = {"message": f"Error en lote: {http_err}", "process_id": None, "result_code": "BATCH_ERROR"}
            except Exception as e:
                logging.error(f"Error inesperado procesando el lote para {tracking_id}. Archivos: {batch_filenames}. Error: {e}")
                for f in batch:
                    final_results_map[f['filename']] = {"message": f"Error inesperado en lote: {e}", "process_id": None, "result_code": "UNEXPECTED_BATCH_ERROR"}

        
        payload["cavali_results"] = final_results_map
        
        next_message_data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(TOPIC_INVOICES_VALIDATED, next_message_data)
        future.result()

        logging.info(f"CAVALI: {tracking_id} validado y publicado en '{TOPIC_INVOICES_VALIDATED}'.")

    except Exception as e:
        traceback.print_exc()
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    return Response(status_code=status.HTTP_204_NO_CONTENT)