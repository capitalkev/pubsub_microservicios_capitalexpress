# gcp-microservicios/trello-service-2/main.py (Versión Final con HTTP, Idempotencia y Lógica de Negocio COMPLETA)

import os
import requests
import datetime
import traceback
from fastapi import FastAPI, HTTPException
from typing import Dict, Any
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
app = FastAPI(title="Trello Service (HTTP Endpoint)")

# --- Clientes y Variables ---
TRELLO_API_KEY = os.getenv("TRELLO_API_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
TRELLO_BOARD_ID = os.getenv("TRELLO_BOARD_ID")
TRELLO_LIST_ID = os.getenv("TRELLO_LIST_ID")
PENDIENTE_CAVALI = os.getenv("PENDIENTE_CAVALI")
PENDIENTE_CONFORMIDAD = os.getenv("PENDIENTE_CONFORMIDAD")
PENDIENTE_HR = os.getenv("PENDIENTE_HR")
storage_client = storage.Client()

# --- Funciones Auxiliares (de tu código original) ---
def _format_number(num: float) -> str:
    return "{:,.2f}".format(num if num is not None else 0)

def _sanitize_name(name: str) -> str:
    return name.strip() if name else "—"

def download_blob_as_bytes(gs_path: str) -> bytes:
    """Descarga un archivo de GCS como bytes."""
    path_parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name, blob_path = path_parts[0], path_parts[1]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()

def card_exists(operation_id: str) -> bool:
    """Verifica si ya existe una tarjeta que contenga el ID de la operación."""
    print(f"TRELLO: Verificando si ya existe tarjeta para la operación: {operation_id}")
    url = f"https://api.trello.com/1/search"
    query = f'"{operation_id}" board:{TRELLO_BOARD_ID}'
    params = {
        'key': TRELLO_API_KEY, 'token': TRELLO_TOKEN, 'query': query,
        'modelTypes': 'cards', 'card_fields': 'name', 'cards_limit': 1
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        if response.json().get('cards'):
            print(f"TRELLO: Tarjeta encontrada para op {operation_id}. No se creará una nueva.")
            return True
    except requests.exceptions.RequestException as e:
        print(f"ADVERTENCIA: Falló la búsqueda en Trello. Se procederá a crear la tarjeta. Error: {e}")
    
    print(f"TRELLO: No se encontraron tarjetas para la op {operation_id}.")
    return False

# ==============================================================================
# LÓGICA DE NEGOCIO ORIGINAL (PROPORCIONADA POR TI)
# ==============================================================================
def process_operation_and_create_card(payload: Dict[str, Any]):
    print("--- 1. Iniciando procesamiento de tarjeta ---")
    operation_id = payload.get("operation_id")
    invoices = payload.get("parsed_results", [])
    if not invoices:
        print(f"ERROR: La lista de 'invoices' en el payload para op {operation_id} está vacía.")
        raise ValueError("No se puede crear una tarjeta sin facturas.")

    client_name = invoices[0].get("client_name")
    metadata = payload.get("metadata", {})
    tasa = metadata.get("tasaOperacion", "N/A")
    comision = metadata.get("comision", "N/A")
    drive_folder_url = payload.get("drive_folder_url", "")
    
    gcs_paths = payload.get("gcs_paths", {})
    attachment_paths = gcs_paths.get("pdf", []) + gcs_paths.get("respaldo", [])

    cavali_results = payload.get("cavali_results", {})
    email = payload.get("user_email", "No disponible")
    nombre_ejecutivo = email.split('@')[0].replace('.', ' ').title()
    siglas_nombre = ''.join([palabra[0] for palabra in nombre_ejecutivo.split()]).upper()

    solicitudAdelanto = metadata.get("solicitudAdelanto", {})
    porcentajeAdelanto = solicitudAdelanto.get("porcentaje", 0)
    
    cuentasDesembolso = metadata.get("cuentasDesembolso", [{}])
    cuenta_principal = cuentasDesembolso[0] if cuentasDesembolso else {}
    desembolso_numero = cuenta_principal.get("numero", "N/A")
    desembolso_moneda = cuenta_principal.get("moneda", "N/A")
    desembolso_tipo = cuenta_principal.get("tipo", "N/A")
    desembolso_banco = cuenta_principal.get("banco", "N/A")
    
    net_total = sum(float(inv.get("net_amount", 0.0)) for inv in invoices)
    debtors_info = {inv['debtor_ruc']: inv['debtor_name'] for inv in invoices}
    currency = invoices[0].get("currency", "PEN")
    
    debtors_str = ', '.join(_sanitize_name(name) for name in debtors_info.values() if name) or 'Ninguno'
    amount_str = f"{currency} {_format_number(net_total)}"
    current_date = datetime.datetime.now().strftime('%d.%m')
    
    card_title = (f"🤖 {current_date} // CLIENTE: {_sanitize_name(client_name)} // DEUDOR: {debtors_str} // MONTO: {amount_str} // {siglas_nombre}// OP: {operation_id}")
    
    debtors_markdown = '\n'.join(f"- RUC {ruc}: {_sanitize_name(name)}" for ruc, name in debtors_info.items()) or '- Ninguno'
    
    cavali_status_lines = []
    for inv in invoices:
        lookup_key = inv.get('xml_filename', 'ID no encontrado') 
        doc_id = inv.get('document_id', 'N/A')
        cavali_info = cavali_results.get(lookup_key, {}) 
        cavali_message = cavali_info.get("message", "Respuesta no disponible")
        cavali_status_lines.append(f"- {doc_id}: *{cavali_message}*")
    cavali_markdown = "\n".join(cavali_status_lines) if cavali_status_lines else "- No se procesó en Cavali."

    # --- TU LÓGICA EXACTA PARA LA DESCRIPCIÓN ---
    card_description_anticipo = f"""
# ANTICIPO PROPUESTO: {porcentajeAdelanto} %

**ID Operación:** {operation_id}

**Deudores:**
{debtors_markdown}

**Tasa:** {tasa}
**Comisión:** {comision}
**Monto Operación:** {amount_str}
**Carpeta Drive:** [Abrir en Google Drive]({drive_folder_url})

### CAVALI:
{cavali_markdown}

### Cuenta bancaria:
- **Banco:** {desembolso_banco}
- **N°cuenta:** {desembolso_numero}
- **Tipo cuenta:** {desembolso_tipo}
"""
    
    card_description_sin_anticipo = f"""
**ID Operación:** {operation_id}

**Deudores:**
{debtors_markdown}

**Tasa:** {tasa}
**Comisión:** {comision}
**Monto Operación:** {amount_str}
**Carpeta Drive:** [Abrir en Google Drive]({drive_folder_url})

### CAVALI:
{cavali_markdown}

### Cuenta bancaria:
- **Banco:** {desembolso_banco}
- **N°cuenta:** {desembolso_numero}
- **Tipo cuenta:** {desembolso_tipo}
"""
    if porcentajeAdelanto > 0:
        card_description = card_description_anticipo
    else:            
        card_description = card_description_sin_anticipo

    # --- FIN DE TU LÓGICA DE DESCRIPCIÓN ---

    id_labels_str = ",".join(filter(None, [PENDIENTE_HR, PENDIENTE_CONFORMIDAD, PENDIENTE_CAVALI]))
    auth_params = {'key': TRELLO_API_KEY, 'token': TRELLO_TOKEN}
    card_payload = {
        'idList': TRELLO_LIST_ID, 'name': card_title, 'desc': card_description, 'idLabels': id_labels_str
    }
    
    print("\n--- 2. Llamando a la API de Trello ---")
    url_card = "https://api.trello.com/1/cards"
    response = requests.post(url_card, params=auth_params, json=card_payload)
    response.raise_for_status() 
    
    card_id = response.json()["id"]
    print(f"--- 3. Tarjeta creada: {card_id} ---")

    url_attachment = f"https://api.trello.com/1/cards/{card_id}/attachments"
    for path in attachment_paths:
        try:
            file_bytes = download_blob_as_bytes(path)
            filename = os.path.basename(path)
            files = {"file": (filename, file_bytes)}
            requests.post(url_attachment, params=auth_params, files=files)
        except Exception as e:
            print(f"ADVERTENCIA: No se pudo adjuntar {path}. Error: {e}")
            
    return card_id

# ==============================================================================
# NUEVO ENDPOINT HTTP
# ==============================================================================
@app.post("/create-card")
def create_trello_card_endpoint(payload: Dict[str, Any]):
    operation_id = payload.get("operation_id")
    if not operation_id:
        raise HTTPException(status_code=400, detail="Falta 'operation_id' en el payload")

    if card_exists(operation_id):
        return {"status": "skipped", "message": f"La tarjeta para la operación {operation_id} ya existe."}

    try:
        card_id = process_operation_and_create_card(payload)
        return {"status": "created", "card_id": card_id}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creando la tarjeta en Trello: {str(e)}")