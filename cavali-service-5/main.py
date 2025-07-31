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
        
        logging.info(f"CAVALI: Procesando {tracking_id} con {len(xml_paths)} XMLs.")

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
        
        invoice_xml_list = [{"name": f['filename'], "fileXml": f['content_base64']} for f in xml_files_b64_group]
        payload_bloqueo = {"invoiceXMLDetail": {"invoiceXML": invoice_xml_list}}
        response_bloqueo = requests.post(CAVALI_BLOCK_URL, json=payload_bloqueo, headers=headers, timeout=300)
        response_bloqueo.raise_for_status()
        bloqueo_data = response_bloqueo.json()
        id_proceso = bloqueo_data.get("response", {}).get("idProceso")

        if not id_proceso:
            raise ValueError(f"Cavali no retornó un idProceso. Respuesta: {bloqueo_data}")

        time.sleep(7) # Espera recomendada por Cavali
        
        payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
        response_estado = requests.post(CAVALI_STATUS_URL, json=payload_estado, headers=headers, timeout=300)
        response_estado.raise_for_status()
        cavali_response_data = response_estado.json()

        results_map = {}
        invoice_details = cavali_response_data.get("response", {}).get("Process", {}).get("ProcessInvoiceDetail", {}).get("Invoice", [])
        for invoice in invoice_details:
            # Lógica para mapear el resultado al nombre de archivo original
            nombre_archivo_original = "desconocido"
            for f in xml_files_b64_group:
                if (str(invoice.get("ruc", "")) in f["filename"] and
                    invoice.get("serie", "") in f["filename"] and
                    str(invoice.get("numeration", "")) in f["filename"]):
                    nombre_archivo_original = f["filename"]
                    break
            results_map[nombre_archivo_original] = {
                "message": invoice.get("message"), "process_id": id_proceso, "result_code": invoice.get("resultCode")
            }
        
        payload["cavali_results"] = results_map
        
        next_message_data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(TOPIC_INVOICES_VALIDATED, next_message_data)
        future.result()

        logging.info(f"CAVALI: {tracking_id} validado y publicado en '{TOPIC_INVOICES_VALIDATED}'.")

    except Exception as e:
        traceback.print_exc()
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
