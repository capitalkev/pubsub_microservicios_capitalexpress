import os
import json
import base64
import traceback
from fastapi import FastAPI, Request, Response, status, HTTPException
from google.cloud import storage, pubsub_v1
from parser import extract_invoice_data

app = FastAPI(title="Parser Service (Pub/Sub Enabled)")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

TOPIC_INVOICES_PARSED = publisher.topic_path(GCP_PROJECT_ID, "invoices-parsed")

def read_xml_from_gcs(gcs_path):
    parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name, file_path = parts
    blob = storage_client.bucket(bucket_name).blob(file_path)
    return blob.download_as_bytes()

@app.post("/pubsub-handler", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_handler(request: Request):
    body = await request.json()
    if not body or "message" not in body:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido.")

    try:
        message_data = base64.b64decode(body["message"]["data"]).decode("utf-8")
        payload = json.loads(message_data)
        
        tracking_id = payload.get("tracking_id")
        xml_paths = payload.get("gcs_paths", {}).get("xml", [])
        
        print(f"PARSER: Procesando {tracking_id} con {len(xml_paths)} XMLs.")

        parsed_results = []
        for xml_path in xml_paths:
            try:
                xml_bytes = read_xml_from_gcs(xml_path)
                invoice_data = extract_invoice_data(xml_bytes)
                invoice_data['xml_filename'] = os.path.basename(xml_path)
                parsed_results.append(invoice_data)
            except Exception as e:
                print(f"PARSER: Error al procesar {xml_path} para {tracking_id}: {e}")
                continue
        
        payload["parsed_results"] = parsed_results
        
        next_message_data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(TOPIC_INVOICES_PARSED, next_message_data)
        future.result()

        print(f"PARSER: {tracking_id} parseado y publicado en '{TOPIC_INVOICES_PARSED}'.")

    except Exception as e:
        traceback.print_exc()
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/parse-direct")
async def parse_direct(request: Request):
    """Endpoint directo para procesamiento síncrono desde orquestador"""
    try:
        operation_data = await request.json()
        tracking_id = operation_data["tracking_id"]
        xml_paths = operation_data.get("gcs_paths", {}).get("xml", [])
        
        print(f"PARSER DIRECTO: Procesando {tracking_id} con {len(xml_paths)} XMLs.")
        
        parsed_invoices = []
        for xml_path in xml_paths:
            try:
                xml_content = read_xml_from_gcs(xml_path)
                invoice_data = extract_invoice_data(xml_content)
                invoice_data['xml_filename'] = xml_path.split('/')[-1]
                parsed_invoices.append(invoice_data)
            except Exception as e:
                print(f"PARSER DIRECTO: Error procesando {xml_path}: {e}")
                continue
        
        result = {"parsed_results": parsed_invoices}
        print(f"PARSER DIRECTO: {tracking_id} procesado exitosamente con {len(parsed_invoices)} facturas.")
        
        return result
        
    except Exception as e:
        print(f"PARSER DIRECTO: Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
