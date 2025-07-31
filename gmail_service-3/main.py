import os, base64, mimetypes, io, json, traceback, requests
import base64
import mimetypes
import io
import pandas as pd
import json
import traceback
from fastapi import FastAPI, Request, Response, status, HTTPException
from email.message import EmailMessage
from dotenv import load_dotenv
from collections import defaultdict
from typing import List, Dict
from datetime import datetime
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
from openpyxl.utils import get_column_letter


load_dotenv()

# --- Configuración ---
USER_TOKEN_FILE = 'token.json'
# --- LÍNEA CORREGIDA ---
# Esta variable es necesaria para que la función get_storage_client funcione.
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SENDER_USER_ID = 'kevin.gianecchine@capitalexpress.cl'
EXCEL_SERVICE_URL = os.getenv("EXCEL_SERVICE_URL")
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/devstorage.read_only']
FIXED_CC_LIST = [
    'kevin.gianecchine@capitalexpress.cl', 'jenssy.huaman@capitalexpress.pe',
    'jakeline.quispe@capitalexpress.pe', 'jhonny.celay@capitalexpress.pe',
    'kevin.tupac@capitalexpress.cl'
]

RUC_GLORIA = [
    "20100190797", "20600679164", "20312372895", "20524088739", "20467539842",
    "20506475288", "20418453177", "20512613218", "20115039262", "20100814162",
    "20518410858", "20101927904", "20479079006", "20100223555", "20532559147",
    "20487268870", "20562613545", "20478963719", "20481694907", "20454629516",
    "20512415840", "20602903193", "20392965191", "20601225639", "20547999691",
    "20600180631", "20116225779", "20131823020", "20601226015", "20131867744",
    "20603778180", "20131835621", "20511866210", "20481640483"
]
app = FastAPI(title="Gmail Service (Pub/Sub Enabled)")

# ==============================================================================
# LÓGICA DE NEGOCIO (FUNCIONES AUXILIARES)
# ==============================================================================

def get_user_credentials():
    """Obtiene las credenciales de usuario desde el archivo token.json."""
    creds = None
    if os.path.exists(USER_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(USER_TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request as GoogleRequest
            creds.refresh(GoogleRequest())
        else:
            raise Exception("No se encontraron credenciales de usuario válidas o el token ha expirado y no se puede refrescar.")
    return creds

def create_gloria_excel(invoice_data_list: List[Dict]) -> (str, bytes): # type: ignore
    """Genera un archivo Excel para Gloria a partir de una lista de diccionarios de facturas."""
    if not invoice_data_list:
        return None, None
    
    data_rows = []
    for invoice in invoice_data_list:
        row = {
            'FACTOR': 20603596294,
            'FECHA DE ENVIO': pd.to_datetime('today').strftime('%d/%m/%Y'),
            'RUC PROVEEDOR': invoice.get('debtor_ruc'),
            'PROVEEDOR': invoice.get('debtor_name'),
            'RUC CLIENTE': invoice.get('client_ruc'),
            'CLIENTE': invoice.get('client_name'),
            'FECHA DE EMISION': pd.to_datetime(invoice.get('issue_date'), errors='coerce').strftime('%d/%m/%Y'),
            'NUM FACTURA': invoice.get('document_id'),
            'IMPORTE NETO PAGAR': invoice.get('net_amount'),
            'MONEDA': invoice.get('currency'),
            'FECHA DE VENCIMIENTO': pd.to_datetime(invoice.get('due_date'), errors='coerce').strftime('%d/%m/%Y')
        }
        data_rows.append(row)

    df = pd.DataFrame(data_rows)
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Facturas', index=False)
        worksheet = writer.sheets['Facturas']

        # Definición de estilos
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        thin_border_side = Side(border_style="thin", color="000000")
        cell_border = Border(left=thin_border_side, right=thin_border_side, top=thin_border_side, bottom=thin_border_side)
        center_alignment = Alignment(horizontal='center', vertical='center')
        right_alignment = Alignment(horizontal='right', vertical='center')

        for row in worksheet.iter_rows():
            for cell in row:
                cell.border = cell_border
                cell.alignment = center_alignment
        
        for cell in worksheet[1]:
            cell.font = header_font
            cell.fill = header_fill

        for col_idx, column_cells in enumerate(worksheet.columns, 1):
            max_length = 0
            column_letter = get_column_letter(col_idx)
            for cell in column_cells:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        for cell in worksheet['I'][1:]:
            cell.number_format = '#,##0.00'
            cell.alignment = right_alignment

    excel_bytes = output_buffer.getvalue()
    filename = f"CapitalExpress_{datetime.now().strftime('%d%m%Y')}.xlsx"
    return filename, excel_bytes

def create_html_body(invoice_data_list: List[Dict]) -> str:
    """Crea el cuerpo HTML del correo a partir de una lista de diccionarios de facturas."""
    first_invoice = invoice_data_list[0]
    client_name = first_invoice.get('client_name')
    client_ruc = first_invoice.get('client_ruc')
    
    df = pd.DataFrame(invoice_data_list)
    df['total_amount'] = df.apply(lambda row: f"{row.get('currency', '')} {float(row.get('total_amount', 0)):,.2f}".strip(), axis=1)
    df['net_amount'] = df.apply(lambda row: f"{row.get('currency', '')} {float(row.get('net_amount', 0)):,.2f}".strip(), axis=1)
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    df_display = df.rename(columns={
        'debtor_ruc': 'RUC Deudor', 'debtor_name': 'Nombre Deudor', 'document_id': 'Documento',
        'total_amount': 'Monto Factura', 'net_amount': 'Monto Neto', 'due_date': 'Fecha de Pago'
    })
    display_columns = ['RUC Deudor', 'Nombre Deudor', 'Documento', 'Monto Factura', 'Monto Neto', 'Fecha de Pago']
    tabla_html = df_display[display_columns].to_html(index=False, border=1, justify='left', classes='invoice_table')
    
    mensaje_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, Helvetica, sans-serif; font-size: 13px; color: #000; }}
        .container {{ max-width: 800px; }} p, li {{ line-height: 1.5; }} ol {{ padding-left: 30px; }}
        table.invoice_table {{ width: 100%; border-collapse: collapse; margin-top: 15px; margin-bottom: 20px; }}
        table.invoice_table th, table.invoice_table td {{ border: 1px solid #777; padding: 6px; text-align: left; font-size: 12px; }}
        table.invoice_table th {{ background-color: #f0f0f0; font-weight: bold; }}
        .disclaimer {{ font-style: italic; font-size: 11px; margin-top: 25px; }}
    </style>
    </head>
    <body>
    <div class="container">
        <p>Estimados señores,</p>
        <p>Por medio de la presente, les informamos que los señores de <strong>{client_name}</strong>, nos han transferido la(s) siguiente(s) factura(s) negociable(s). Solicitamos su amable confirmación sobre los siguientes puntos:</p>
        <ol>
            <li>¿La(s) factura(s) ha(n) sido recepcionada(s) conforme con sus productos o servicios?</li>
            <li>¿Cuál es la fecha programada para el pago de la(s) misma(s)?</li>
            <li>Por favor, confirmar el Monto Neto a pagar, considerando detracciones, retenciones u otros descuentos.</li>
        </ol>
        <p><strong>Detalle de las facturas:</strong></p>
        <p>Cliente: {client_name}<br>
        RUC Cliente: {client_ruc}
        </p>
        {tabla_html}
        <p>Agradecemos de antemano su pronta respuesta. Con su confirmación, procederemos a la anotación en cuenta en CAVALI.</p>
        <p class="disclaimer">"Sin perjuicio de lo anteriormente mencionado, nos permitimos recordarles que toda acción tendiente a 
        simular la emisión de la referida factura negociable o letra para obtener un beneficio a título personal o a favor de la otra 
        parte de la relación comercial, teniendo pleno conocimiento de que la misma no proviene de una relación comercial verdadera, 
        se encuentra sancionada penalmente como delito de estafa en nuestro ordenamiento jurídico.
        Asimismo, en caso de que vuestra representada cometa un delito de forma conjunta y/o en contubernio con el emitente de la factura, 
        dicha acción podría tipificarse como delito de asociación ilícita para delinquir, según el artículo 317 del Código Penal, por lo 
        que nos reservamos el derecho de iniciar las acciones penales correspondientes en caso resulte necesario"</p>
    </div>
    </body>
    </html>
    """
    return mensaje_html

# ==============================================================================
# ENDPOINT DE PUBSUB
# ==============================================================================
@app.post("/pubsub-handler", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_handler(request: Request):
    body = await request.json()
    if not body or "message" not in body:
        raise HTTPException(status_code=400, detail="Payload de Pub/Sub inválido.")
    
    try:
        payload = json.loads(base64.b64decode(body["message"]["data"]).decode("utf-8"))
        operation_id = payload.get("operation_id")
        print(f"GMAIL: Recibida notificación para op {operation_id}")

        user_creds = get_user_credentials()
        storage_client = storage.Client(credentials=user_creds)
        gmail_service = build('gmail', 'v1', credentials=user_creds)

        invoices = payload.get("parsed_results", [])
        if not invoices:
            print(f"WARN: No hay facturas en el payload para op {operation_id}. No se enviará correo.")
            return

        # Agrupar facturas por RUC de deudor
        invoices_by_debtor_ruc = defaultdict(list)
        for inv in invoices:
            invoices_by_debtor_ruc[inv['debtor_ruc']].append(inv)
        
        # --- LÓGICA DE EXCEL AHORA VIVE AQUÍ ---
        for ruc_deudor, facturas_grupo in invoices_by_debtor_ruc.items():
            correos_finales_str = ""
            try:
                # 1. Actualiza el contacto en Excel con el correo del formulario (CORREGIDO)
                nombre_deudor = facturas_grupo[0]['debtor_name']
                correo_operacion_str = payload.get('metadata', {}).get('mailVerificacion', '').strip()
                
                # Divide la cadena de correos en una lista
                correos_operacion = [c.strip() for c in correo_operacion_str.split(';') if c.strip()]

                # Itera sobre cada correo y lo actualiza individualmente
                for correo in correos_operacion:
                    update_payload = {"ruc": ruc_deudor, "correo": correo, "nombre_deudor": nombre_deudor}
                    requests.post(f"{EXCEL_SERVICE_URL}/update-contact", json=update_payload, timeout=20).raise_for_status()

                # 2. Obtiene la lista completa de correos para ese RUC
                response = requests.get(f"{EXCEL_SERVICE_URL}/get-emails/{ruc_deudor}", timeout=20)
                if response.status_code == 200:
                    correos_finales_str = response.json().get("emails", "")
                
            except requests.exceptions.RequestException as e:
                print(f"ADVERTENCIA: Falló la comunicación con Excel para RUC {ruc_deudor}. Error: {e}")
            
            if not correos_finales_str:
                print(f"ADVERTENCIA: No se encontraron correos para el RUC {ruc_deudor} en la op {operation_id}. No se enviará email.")
                continue

            # --- CONSTRUCCIÓN Y ENVÍO DEL CORREO ---
            message = EmailMessage()
            message.add_alternative(create_html_body(facturas_grupo), subtype='html') # Descomenta y usa tu función
            message['To'] = correos_finales_str
            cc_list = set(FIXED_CC_LIST)
            if payload.get("user_email"):
                cc_list.add(payload.get("user_email"))
            message['Cc'] = ",".join(sorted(list(cc_list)))
            message['Subject'] = f"Confirmación de Facturas Negociables - {facturas_grupo[0]['client_name']}"

            # Adjuntar PDFs
            pdf_paths = payload.get("gcs_paths", {}).get("pdf", [])
            for path in pdf_paths:
                try:
                    bucket_name, blob_name = path.replace("gs://", "").split("/", 1)
                    pdf_bytes = storage_client.bucket(bucket_name).blob(blob_name).download_as_bytes()
                    maintype, subtype = (mimetypes.guess_type(os.path.basename(path))[0] or "application/octet-stream").split('/')
                    message.add_attachment(pdf_bytes, maintype=maintype, subtype=subtype, filename=os.path.basename(path))
                except Exception as e:
                    print(f"ADVERTENCIA: al adjuntar el archivo {path}: {e}")
            
            # Envío del correo
            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            gmail_service.users().messages().send(userId=SENDER_USER_ID, body={'raw': encoded_message}).execute()
            print(f"GMAIL: Correo para deudor {ruc_deudor} (Op: {operation_id}) enviado a: {correos_finales_str}")

    except Exception as e:
        traceback.print_exc()
        return Response(status_code=500)
        
    return Response(status_code=status.HTTP_204_NO_CONTENT)
