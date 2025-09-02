import uuid
import json
import os
import base64
import traceback
import requests
from typing import List, Annotated, Optional
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime, timezone
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Request, Response, status, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.cloud import storage, pubsub_v1
import firebase_admin
from firebase_admin import credentials, auth
from database import get_db, engine
from repository import OperationRepository
import models
from pydantic import BaseModel
import logging
import asyncio

models.Base.metadata.create_all(bind=engine)
load_dotenv()

try:
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.ApplicationDefault())
except Exception as e:
    print(f"ADVERTENCIA: Firebase SDK no inicializado: {e}")

app = FastAPI(title="Orquestador de Operaciones")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", 
        "https://localhost:5173",
        "http://127.0.0.1:5173", 
        "https://operaciones-peru.web.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL")
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL")
GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL")
PARSER_SERVICE_URL = os.getenv("PARSER_SERVICE_URL")
CAVALI_SERVICE_URL = os.getenv("CAVALI_SERVICE_URL")

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
bucket = storage_client.bucket(BUCKET_NAME)

TOPIC_OPERATION_SUBMITTED = publisher.topic_path(GCP_PROJECT_ID, "operation-submitted")
TOPIC_OPERATION_PERSISTED = publisher.topic_path(GCP_PROJECT_ID, "operation-persisted")


async def get_current_user(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token de autorización inválido")
    try:
        decoded_token = auth.verify_id_token(authorization.split("Bearer ")[1])
        email = decoded_token['email']
        
        user_in_db = db.query(models.Usuario).filter(models.Usuario.email == email).first()
        
        if not user_in_db:
            repo = OperationRepository(db)
            repo.update_and_get_last_login(email, decoded_token.get('name', ''))
            user_in_db = db.query(models.Usuario).filter(models.Usuario.email == email).first()

        decoded_token['role'] = user_in_db.rol 
        return decoded_token
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido o error de base de datos: {e}")

# Receptor de operaciones del frontend
@app.post("/submit-operation", status_code=status.HTTP_202_ACCEPTED)
async def submit_operation_async(
    metadata_str: Annotated[str, Form(alias="metadata")],
    xml_files: Annotated[List[UploadFile], File(alias="xml_files")],
    pdf_files: Annotated[List[UploadFile], File(alias="pdf_files")],
    respaldo_files: Annotated[List[UploadFile], File(alias="respaldo_files")],
    user: dict = Depends(get_current_user), db: Session = Depends(get_db)
):
    try:
        metadata = json.loads(metadata_str)
        tracking_id = str(uuid.uuid4())
        upload_folder = f"operations/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/{tracking_id}"
        def upload_file(file: UploadFile, subfolder: str) -> str:
            blob_path = f"{upload_folder}/{subfolder}/{file.filename}"; blob = bucket.blob(blob_path); blob.upload_from_file(file.file); return f"gs://{BUCKET_NAME}/{blob_path}"
        
        gcs_paths = { "xml": [upload_file(f, "xml") for f in xml_files], "pdf": [upload_file(f, "pdf") for f in pdf_files], "respaldo": [upload_file(f, "respaldos") for f in respaldo_files] }
        operation_data = { "tracking_id": tracking_id, "user_email": user['email'], "metadata": metadata, "gcs_paths": gcs_paths }
        
        # Procesar directamente Parser y Cavali, Drive en paralelo
        await process_operation_sync(operation_data, db)
        return {"status": "processing", "tracking_id": tracking_id}
    except Exception as e:
        traceback.print_exc(); raise HTTPException(status_code=500, detail=str(e))

async def process_operation_sync(operation_data: dict, db: Session):
    """
    Procesa la operación de forma síncrona:
    1. Parser (secuencial)
    2. Cavali (secuencial, con tolerancia a fallos)
    3. Drive (paralelo, pub/sub)
    4. Finalizar operación
    """
    try:
        tracking_id = operation_data["tracking_id"]
        logging.info(f"SYNC: Iniciando procesamiento de {tracking_id}")
        
        # 1. Llamar Parser directamente
        parsed_results = await call_parser_service(operation_data)
        if not parsed_results:
            logging.error(f"SYNC: Parser falló para {tracking_id}")
            raise Exception("Parser service failed")
        
        # 2. Llamar Cavali directamente (con tolerancia a fallos)
        cavali_results = await call_cavali_service(operation_data)
        if not cavali_results:
            logging.warning(f"SYNC: Cavali falló para {tracking_id}, continuando sin validación")
            cavali_results = {}
        
        # 3. Publicar a Drive en paralelo
        drive_payload = {**operation_data}
        publisher.publish(TOPIC_OPERATION_SUBMITTED, json.dumps(drive_payload).encode("utf-8")).result()
        
        # 4. Esperar resultado de Drive y finalizar
        await wait_for_drive_and_finalize(tracking_id, operation_data, parsed_results, cavali_results, db)
        
    except Exception as e:
        logging.error(f"SYNC: Error procesando {operation_data.get('tracking_id')}: {e}")
        traceback.print_exc()
        raise

async def call_parser_service(operation_data: dict) -> dict:
    """Llama al parser service directamente"""
    try:
        if not PARSER_SERVICE_URL:
            logging.error("PARSER_SERVICE_URL no configurada")
            return {}
            
        url = f"{PARSER_SERVICE_URL}/parse-direct"
        headers = {"Content-Type": "application/json"}
        
        async with asyncio.timeout(300):  # 5 min timeout
            response = requests.post(url, json=operation_data, headers=headers, timeout=300)
            response.raise_for_status()
            result = response.json()
            logging.info(f"PARSER: Éxito para {operation_data['tracking_id']}")
            return result.get("parsed_results", {})
            
    except Exception as e:
        logging.error(f"PARSER: Error para {operation_data['tracking_id']}: {e}")
        return {}

async def call_cavali_service(operation_data: dict) -> dict:
    """Llama al cavali service directamente con tolerancia a fallos"""
    try:
        if not CAVALI_SERVICE_URL:
            logging.warning("CAVALI_SERVICE_URL no configurada, continuando sin validación")
            return {}
            
        url = f"{CAVALI_SERVICE_URL}/validate-direct"
        headers = {"Content-Type": "application/json"}
        
        async with asyncio.timeout(600):  # 10 min timeout
            response = requests.post(url, json=operation_data, headers=headers, timeout=600)
            response.raise_for_status()
            result = response.json()
            logging.info(f"CAVALI: Éxito para {operation_data['tracking_id']}")
            return result.get("cavali_results", {})
            
    except Exception as e:
        logging.warning(f"CAVALI: Error para {operation_data['tracking_id']}: {e}, continuando sin validación")
        return {}

async def wait_for_drive_and_finalize(tracking_id: str, operation_data: dict, parsed_results: dict, cavali_results: dict, db: Session):
    """Espera el resultado de Drive y finaliza la operación"""
    try:
        # Insertar datos iniciales en staging
        stmt = pg_insert(models.OperationStaging).values(
            tracking_id=tracking_id, 
            initial_payload=operation_data,
            parsed_data=parsed_results,
            cavali_data=cavali_results
        )
        db.execute(stmt)
        db.commit()
        
        # El aggregator se encargará de finalizar cuando llegue drive_data
        logging.info(f"SYNC: Datos de Parser y Cavali guardados para {tracking_id}, esperando Drive")
        
    except Exception as e:
        logging.error(f"SYNC: Error guardando datos para {tracking_id}: {e}")
        raise

# Endpoint para recibir datos de Pub/Sub y agregarlos a la operación
@app.post("/pubsub-aggregator", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_aggregator(request: Request, db: Session = Depends(get_db)):
    body = await request.json(); message = body.get("message", {})
    if not message or "data" not in message: raise HTTPException(status_code=400)
    try:
        payload = json.loads(base64.b64decode(message["data"]).decode("utf-8")); tracking_id = payload["tracking_id"]
        # Solo manejamos Drive ahora, Parser y Cavali se procesan síncronamente
        if "drive_folder_url" in payload:
            # Actualizar con datos de Drive
            update_data = {"drive_data": {"drive_folder_url": payload["drive_folder_url"]}}
            stmt = pg_insert(models.OperationStaging).values(tracking_id=tracking_id, **update_data).on_conflict_do_update(index_elements=['tracking_id'], set_=update_data)
            db.execute(stmt)
            
            # Verificar si ya tenemos todos los datos (parser, cavali ya están, solo faltaba drive)
            staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).with_for_update().first()
            if staging_record and staging_record.parsed_data is not None and staging_record.drive_data:
                logging.info(f"AGGREGATOR: Todos los datos para {tracking_id} recibidos. Procesando.")
                final_payload = staging_record.initial_payload
                final_payload.update({
                    "parsed_results": staging_record.parsed_data,
                    "cavali_results": staging_record.cavali_data or {},
                    "drive_folder_url": staging_record.drive_data["drive_folder_url"]
                })
                process_final_operation(final_payload, db)
                db.delete(staging_record)
        else:
            logging.warning(f"AGGREGATOR: Payload inesperado para {tracking_id}. Payload keys: {list(payload.keys())}")
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        db.commit()
    except Exception as e:
        db.rollback(); traceback.print_exc(); raise
    return Response(status_code=status.HTTP_204_NO_CONTENT)

def process_final_operation(payload: dict, db: Session):
    repo = OperationRepository(db)
    original_tracking_id = payload["tracking_id"]
    
    # 1. Filtrar facturas válidas
    valid_invoices = [inv for inv in payload["parsed_results"] 
                     if inv.get("valid", True) and not inv.get("error")]
    
    if not valid_invoices:
        print(f"FINALIZER: No hay facturas válidas para {original_tracking_id}")
        return
    
    print(f"FINALIZER: {len(valid_invoices)} facturas válidas de {len(payload['parsed_results'])} totales")
    
    # 2. Verificar duplicados usando fingerprint
    duplicate_check = repo.check_duplicate_invoices(valid_invoices)
    
    if duplicate_check['has_duplicates']:
        print(f"FINALIZER: Detectados {len(duplicate_check['duplicates'])} duplicados para {original_tracking_id}:")
        for dup in duplicate_check['duplicates']:
            print(f"  - {dup['fingerprint']} ya existe en operación {dup['existing_operation']}")
        
        # Decidir qué hacer con duplicados
        if not duplicate_check['new_invoices']:
            print(f"FINALIZER: Todas las facturas son duplicadas, rechazando operación {original_tracking_id}")
            return  # Rechazar si TODAS son duplicadas
        
        print(f"FINALIZER: Procesando solo {len(duplicate_check['new_invoices'])} facturas nuevas")
        valid_invoices = duplicate_check['new_invoices']
    
    # 3. Agrupar por moneda (solo facturas nuevas y válidas)
    invoices_by_currency = defaultdict(list)
    for inv in valid_invoices:
        currency = inv.get('currency')
        if currency in {'PEN', 'USD', 'EUR'}:  # Monedas válidas
            invoices_by_currency[currency].append(inv)
    
    if not invoices_by_currency:
        print(f"FINALIZER: No hay facturas con monedas válidas para {original_tracking_id}")
        return

    # 4. Crear operaciones por moneda
    for currency, invoices_in_group in invoices_by_currency.items():
        # Generar ID de operación con formato OP-YYYYMMDD-XXX
        operation_id = repo.generar_siguiente_id_operacion()
            
        print(f"FINALIZER: Creando operación {operation_id} para {len(invoices_in_group)} facturas en {currency}")
        
        repo.save_full_operation(
            operation_id=operation_id,
            metadata=payload['metadata'], 
            drive_url=payload['drive_folder_url'],
            invoices_data=invoices_in_group,
            cavali_results_map=payload['cavali_results']
        )
        print(f"FINALIZER: Operación {operation_id} guardada en DB.")

        notification_payload = {
            "operation_id": operation_id,
            "idempotency_key": f"{operation_id}_{currency}",
            "user_email": payload.get("user_email"), 
            "metadata": payload.get("metadata"),
            "drive_folder_url": payload.get("drive_folder_url"), 
            "cavali_results": payload.get("cavali_results"),
            "parsed_results": invoices_in_group, 
            "gcs_paths": payload.get("gcs_paths"),
            "duplicate_info": {
                "duplicates_found": len(duplicate_check['duplicates']),
                "duplicates_details": duplicate_check['duplicates']
            },
            "original_tracking_id": original_tracking_id
        }

        # Enviar notificación a Gmail
        print(f"FINALIZER: Gmail URL configurada: {GMAIL_SERVICE_URL}")
        try:
            if GMAIL_SERVICE_URL:
                full_gmail_url = f"{GMAIL_SERVICE_URL}/send-email"
                print(f"FINALIZER: Llamando a Gmail URL: {full_gmail_url}")
                print(f"FINALIZER: Payload para Gmail: operation_id={operation_id}, parsed_results={len(notification_payload.get('parsed_results', []))}")
                
                gmail_response = requests.post(full_gmail_url, json=notification_payload, timeout=300)
                print(f"FINALIZER: Gmail respondió con status code: {gmail_response.status_code}")
                gmail_response.raise_for_status()
                print(f"FINALIZER: Llamada a Gmail para op {operation_id} completada con éxito.")
            else:
                print("ADVERTENCIA: GMAIL_SERVICE_URL no está configurada.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falló la llamada a Gmail para op {operation_id}. Error: {e}")
            print(f"ERROR: Tipo de error: {type(e).__name__}")
            if hasattr(e, 'response') and e.response:
                print(f"GMAIL-SERVICE respondió con status: {e.response.status_code}")
                print(f"GMAIL-SERVICE respondió con: {e.response.text}")
                
        # Enviar notificación a Trello
        print(f"FINALIZER: Trello URL configurada: {TRELLO_SERVICE_URL}")
        try:
            if TRELLO_SERVICE_URL:
                full_trello_url = f"{TRELLO_SERVICE_URL}/create-card"
                print(f"FINALIZER: Llamando a Trello URL: {full_trello_url}")
                print(f"FINALIZER: Payload para Trello: operation_id={operation_id}")
                
                trello_response = requests.post(full_trello_url, json=notification_payload, timeout=300)
                print(f"FINALIZER: Trello respondió con status code: {trello_response.status_code}")
                trello_response.raise_for_status()
                print(f"FINALIZER: Llamada a Trello para op {operation_id} completada con éxito.")
            else:
                print("ADVERTENCIA: TRELLO_SERVICE_URL no está configurada.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falló la llamada a Trello para op {operation_id}. Error: {e}")
            print(f"ERROR: Tipo de error: {type(e).__name__}")
            if hasattr(e, 'response') and e.response:
                print(f"TRELLO-SERVICE respondió con status: {e.response.status_code}")
                print(f"TRELLO-SERVICE respondió con: {e.response.text}")

        

        publisher.publish(TOPIC_OPERATION_PERSISTED, json.dumps(notification_payload).encode("utf-8")).result()
        print(f"FINALIZER: Notificación para Gmail (Op: {operation_id}) publicada.")

# ROL 3: ENDPOINTS DE CONSULTA

@app.get("/operation-status/{tracking_id}")
async def get_operation_status(tracking_id: str, db: Session = Depends(get_db)):
    staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).first()
    if not staging_record:
        raise HTTPException(status_code=404, detail="Operación no encontrada en la etapa de procesamiento.")
    drive_info = staging_record.drive_data or {}
    return {"drive_folder_url": drive_info.get("drive_folder_url")}

@app.get("/api/operaciones")
async def get_user_operations(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    estado: Optional[str] = Query(None, description="Filtrar por estado de operación")
):
    repo = OperationRepository(db)
    last_login = repo.update_and_get_last_login(user['email'], user.get('name', ''))

    user_role = user.get('role')
    offset = (page - 1) * limit
    
    # El método ahora devuelve un diccionario con 'operations' y 'total'
    paginated_result = repo.get_dashboard_operations(user['email'], user_role, offset, limit, estado_filter=estado)

    return {
        "last_login": last_login.isoformat() if last_login else None,
        "operations": paginated_result["operations"],
        "total": paginated_result["total"],
        "page": page,
        "limit": limit
    }

@app.get("/api/operaciones/{op_id}/detalle")
async def get_operation_detail(op_id: str, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    operacion = db.query(models.Operacion).options(
        joinedload(models.Operacion.cliente),
        selectinload(models.Operacion.facturas).joinedload(models.Factura.deudor),
        selectinload(models.Operacion.gestiones).joinedload(models.Gestion.analista)
    ).filter(models.Operacion.id == op_id).first()
    
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    
    # Verificar permisos: admins ven todo, ventas solo sus operaciones
    if user.get('role') != 'admin' and operacion.email_usuario != user['email']:
        raise HTTPException(status_code=403, detail="No tiene permisos para ver esta operación")
    
    return {
        "id": operacion.id,
        "fechaIngreso": operacion.fecha_creacion.isoformat(),
        "cliente": operacion.cliente.razon_social if operacion.cliente else "N/A",
        "deudor": operacion.facturas[0].deudor.razon_social if operacion.facturas and operacion.facturas[0].deudor else "N/A",
        "monto": operacion.monto_sumatoria_total,
        "moneda": operacion.moneda_sumatoria,
        "estado": operacion.estado,
        "emailUsuario": operacion.email_usuario,
        "nombreEjecutivo": operacion.nombre_ejecutivo,
        "urlCarpetaDrive": operacion.url_carpeta_drive,
        "tasa": operacion.tasa_operacion,
        "comision": operacion.comision,
        "gestiones": [{
            "id": g.id,
            "fecha": g.fecha_creacion.isoformat(),
            "tipo": g.tipo,
            "resultado": g.resultado,
            "nombreContacto": g.nombre_contacto,
            "cargoContacto": g.cargo_contacto,
            "telefonoEmailContacto": g.telefono_email_contacto,
            "notas": g.notas,
            "analista": g.analista.nombre if g.analista else "Sistema"
        } for g in operacion.gestiones],
        "facturas": [{
            "folio": f.numero_documento,
            "deudorRuc": f.deudor_ruc,
            "deudorNombre": f.deudor.razon_social if f.deudor else "N/A",
            "fechaEmision": f.fecha_emision.isoformat() if f.fecha_emision else None,
            "fechaVencimiento": f.fecha_vencimiento.isoformat() if f.fecha_vencimiento else None,
            "monto": f.monto_total,
            "moneda": f.moneda,
            "estado": f.estado
        } for f in operacion.facturas]
    }


@app.get("/api/gestiones/operaciones")
async def get_operaciones_gestion(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    repo = OperationRepository(db)
    operaciones_db = repo.get_gestiones_operations(user_email=user['email'], user_role=user.get('role'))
    resultado_formateado = []
    for op in operaciones_db:
        alerta_ia = None
        antiquity_days = (datetime.now(timezone.utc).date() - op.fecha_creacion.date()).days
        if antiquity_days > 3 and len(op.gestiones) == 0:
            alerta_ia = {"tipo": "llamar", "texto": "¡Llamar ya! Operación con más de 3 días sin gestión."}

        estado_operacion = op.estado
        if op.estado == 'Conforme' and not op.adelanto_express:
             pass 

        resultado_formateado.append({
            "id": op.id,
            "cliente": op.cliente.razon_social if op.cliente else "N/A",
            "deudor": op.facturas[0].deudor.razon_social if op.facturas and op.facturas[0].deudor else "N/A",
            "montoTotal": op.monto_sumatoria_total,
            "moneda": op.moneda_sumatoria,
            "fechaIngreso": op.fecha_creacion.isoformat(),
            "antiquity": antiquity_days,
            "correosEnviados": 2, 
            "adelantoExpress": op.adelanto_express,
            "estadoOperacion": estado_operacion,
            "tasa": op.tasa_operacion,
            "comision": op.comision,
            "analistaAsignado": { "nombre": op.analista_asignado.nombre if op.analista_asignado else "Sin Asignar", "email": op.analista_asignado.email if op.analista_asignado else None },
            "gestiones": [{ "id": g.id, "fecha": g.fecha_creacion.isoformat(), "tipo": g.tipo, "resultado": g.resultado, "notas": g.notas, "analista": g.analista.nombre if g.analista else "Sistema" } for g in op.gestiones],
            "facturas": [{ "folio": f.numero_documento, "monto": f.monto_total, "moneda": f.moneda, "estado": f.estado } for f in op.facturas],
            "alertaIA": alerta_ia
        })
    return resultado_formateado

class GestionCreate(BaseModel):
    tipo: str
    resultado: str
    nombre_contacto: str | None = None
    cargo_contacto: str | None = None
    telefono_email_contacto: str | None = None
    notas: str | None = None

@app.post("/api/operaciones/{op_id}/gestiones", status_code=status.HTTP_201_CREATED)
async def registrar_gestion(op_id: str, gestion_data: GestionCreate, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    nueva_gestion = models.Gestion(id_operacion=op_id, analista_email=user['email'], **gestion_data.dict())
    db.add(nueva_gestion)
    db.commit()
    db.refresh(nueva_gestion)
    return nueva_gestion

@app.delete("/api/gestiones/{gestion_id}", status_code=status.HTTP_204_NO_CONTENT)
async def eliminar_gestion(gestion_id: int, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    gestion = db.query(models.Gestion).filter(models.Gestion.id == gestion_id).first()
    if not gestion:
        raise HTTPException(status_code=404, detail="Gestión no encontrada")
    
    # Verificar que el usuario tiene permisos para eliminar la gestión
    if user['role'] != 'admin' and gestion.analista_email != user['email']:
        raise HTTPException(status_code=403, detail="No tiene permisos para eliminar esta gestión")
    
    db.delete(gestion)
    db.commit()
    return

class FacturaUpdate(BaseModel):
    estado: str

@app.patch("/api/operaciones/{op_id}/facturas/{folio}")
async def actualizar_factura_y_operacion(op_id: str, folio: str, update_data: FacturaUpdate, db: Session = Depends(get_db)):
    factura = db.query(models.Factura).filter(
        models.Factura.id_operacion == op_id, models.Factura.numero_documento == folio
    ).first()
    if not factura:
        raise HTTPException(status_code=404, detail="Factura no encontrada")
    factura.estado = update_data.estado
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    facturas_de_operacion = operacion.facturas
    if any(f.estado == 'Rechazada' for f in facturas_de_operacion):
        operacion.estado = 'Discrepancia'
    elif all(f.estado == 'Verificada' for f in facturas_de_operacion):
        operacion.estado = 'pendiente'
    else:
        operacion.estado = 'En Verificación'
    db.commit()
    db.refresh(operacion)
    return {"folio": folio, "nuevoEstadoFactura": factura.estado, "nuevoEstadoOperacion": operacion.estado}

class AdelantoJustificacion(BaseModel):
    justificacion: str

@app.post("/api/operaciones/{op_id}/adelanto-express", status_code=status.HTTP_200_OK)
async def mover_a_adelanto(op_id: str, data: AdelantoJustificacion, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    operacion.adelanto_express = True
    operacion.estado = 'Adelanto'
    nueva_gestion = models.Gestion(
        id_operacion=op_id, analista_email=user['email'], tipo="Adelanto Express",
        resultado="Movido a cola de Adelanto", notas=data.justificacion
    )
    db.add(nueva_gestion)
    db.commit()
    db.refresh(operacion)
    return operacion

@app.patch("/api/operaciones/{op_id}/completar", status_code=status.HTTP_200_OK)
async def completar_operacion(op_id: str, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    operacion.estado = 'Verificada'
    db.commit()
    return {"status": "ok", "message": f"Operación {op_id} marcada como completada."}

@app.patch("/api/operaciones/{op_id}/assign", status_code=status.HTTP_200_OK)
async def asignar_operacion(op_id: str, assignee_email: str, user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    operacion.analista_asignado_email = assignee_email
    db.commit()
    return {"status": "ok", "message": f"Operación {op_id} asignada a {assignee_email}."}

class SendVerificationRequest(BaseModel):
    emails: str
    customMessage: Optional[str] = None

@app.post("/api/operaciones/{op_id}/send-verification", status_code=status.HTTP_200_OK)
async def send_verification_emails(
    op_id: str, 
    request: SendVerificationRequest, 
    user: dict = Depends(get_current_user), 
    db: Session = Depends(get_db)
):
    """Envía correos de verificación manual para una operación específica"""
    # Verificar que la operación existe
    operacion = db.query(models.Operacion).options(
        selectinload(models.Operacion.facturas)
    ).filter(models.Operacion.id == op_id).first()
    
    if not operacion:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    
    if not request.emails.strip():
        raise HTTPException(status_code=400, detail="Debe proporcionar al menos un correo electrónico")
    
    try:
        # Preparar datos para el servicio de Gmail
        facturas_data = []
        for factura in operacion.facturas:
            facturas_data.append({
                "debtor_ruc": factura.ruc_deudor,
                "debtor_name": factura.nombre_deudor,
                "client_ruc": factura.ruc_cliente, 
                "client_name": factura.nombre_cliente,
                "document_id": factura.folio,
                "total_amount": float(factura.monto_factura),
                "net_amount": float(factura.monto_neto),
                "currency": factura.moneda,
                "issue_date": factura.fecha_emision.isoformat(),
                "due_date": factura.fecha_pago.isoformat()
            })
        
        # Payload para el servicio Gmail
        gmail_payload = {
            "operation_id": op_id,
            "parsed_results": facturas_data,
            "user_email": user['email'],
            "metadata": {
                "mailVerificacion": request.emails,
                "customMessage": request.customMessage
            },
            "gcs_paths": {
                "pdf": []  # No adjuntar PDFs en verificaciones manuales
            }
        }
        
        # Llamar al servicio Gmail
        response = requests.post(
            f"{GMAIL_SERVICE_URL}/send-email",
            json=gmail_payload,
            timeout=60
        )
        
        if response.status_code != 200:
            error_detail = response.json().get("detail", "Error desconocido en el servicio de correo")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Error al enviar correos: {error_detail}"
            )
        
        return {
            "status": "success",
            "message": f"Correos de verificación enviados exitosamente a: {request.emails}",
            "details": response.json()
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error comunicándose con Gmail service: {e}")
        raise HTTPException(
            status_code=503,
            detail="Servicio de correo temporalmente no disponible"
        )
    except Exception as e:
        print(f"Error inesperado en send_verification_emails: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error interno del servidor"
        )

class UserSession(BaseModel):
    email: str
    nombre: str | None = None
    rol: str
    ultimo_ingreso: datetime | None = None

@app.get("/api/users/me", response_model=UserSession)
async def get_current_user_session(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Devuelve los detalles del usuario autenticado, incluyendo su rol,
    basado en el token JWT proporcionado.
    """
    user_in_db = db.query(models.Usuario).filter(models.Usuario.email == user['email']).first()
    
    if not user_in_db:
        raise HTTPException(status_code=404, detail="Usuario no encontrado en la base de datos.")

    return UserSession(
        email=user_in_db.email,
        nombre=user_in_db.nombre,
        rol=user_in_db.rol,
        ultimo_ingreso=user_in_db.ultimo_ingreso
    )


@app.get("/api/users/analysts")
async def get_analyst_users(db: Session = Depends(get_db)):
    roles_permitidos = ['gestion', 'admin']
    
    analysts = db.query(models.Usuario).filter(models.Usuario.rol.in_(roles_permitidos)).all()
    return [{"email": u.email, "nombre": u.nombre} for u in analysts]