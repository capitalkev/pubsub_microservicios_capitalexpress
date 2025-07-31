import uuid
import json
import os
import base64
import traceback
import requests
from typing import List, Annotated, Optional
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime, timedelta
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Request, Response, status, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import text, update, case
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.cloud import storage, pubsub_v1
import firebase_admin
from firebase_admin import credentials, auth
from database import get_db, engine
from repository import OperationRepository
import models
from pydantic import BaseModel

models.Base.metadata.create_all(bind=engine)
load_dotenv()

try:
    firebase_admin.initialize_app(credentials.ApplicationDefault())
except Exception as e:
    print(f"ADVERTENCIA: Firebase SDK no inicializado: {e}")

app = FastAPI(title="Orquestador de Operaciones")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "https://operaciones-peru.web.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Clientes y Variables Globales ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL")
# NEW: Define DRIVE_SERVICE_URL
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL")


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
bucket = storage_client.bucket(BUCKET_NAME)

TOPIC_OPERATION_SUBMITTED = publisher.topic_path(GCP_PROJECT_ID, "operation-submitted")
TOPIC_OPERATION_PERSISTED = publisher.topic_path(GCP_PROJECT_ID, "operation-persisted")

# --- Autenticación ---
async def get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "): raise HTTPException(status_code=401)
    try: return auth.verify_id_token(authorization.split("Bearer ")[1])
    except Exception as e: raise HTTPException(status_code=401, detail=f"Token inválido: {e}")

# ==============================================================================
# ROL 1: INICIO DEL FLUJO (FAN-OUT)
# ==============================================================================
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
        tracking_id = str(uuid.uuid4()) # This is the UUID
        upload_folder = f"operations/{datetime.now().strftime('%Y-%m-%d')}/{tracking_id}"
        def upload_file(file: UploadFile, subfolder: str) -> str:
            blob_path = f"{upload_folder}/{subfolder}/{file.filename}"; blob = bucket.blob(blob_path); blob.upload_from_file(file.file); return f"gs://{BUCKET_NAME}/{blob_path}"
        
        pubsub_payload = { "tracking_id": tracking_id, "user_email": user['email'], "metadata": metadata, "gcs_paths": { "xml": [upload_file(f, "xml") for f in xml_files], "pdf": [upload_file(f, "pdf") for f in pdf_files], "respaldo": [upload_file(f, "respaldos") for f in respaldo_files] } }
        stmt = pg_insert(models.OperationStaging).values(tracking_id=tracking_id, initial_payload=pubsub_payload)
        db.execute(stmt); db.commit()
        publisher.publish(TOPIC_OPERATION_SUBMITTED, json.dumps(pubsub_payload).encode("utf-8")).result()
        return {"status": "processing", "tracking_id": tracking_id}
    except Exception as e:
        traceback.print_exc(); raise HTTPException(status_code=500, detail=str(e))

# ==============================================================================
# ROL 2: AGREGADOR (FAN-IN)
# ==============================================================================
@app.post("/pubsub-aggregator", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_aggregator(request: Request, db: Session = Depends(get_db)):
    body = await request.json(); message = body.get("message", {})
    if not message or "data" not in message: raise HTTPException(status_code=400)
    try:
        payload = json.loads(base64.b64decode(message["data"]).decode("utf-8")); tracking_id = payload["tracking_id"]
        update_data = {}
        if "parsed_results" in payload: update_data["parsed_data"] = payload["parsed_results"]
        elif "cavali_results" in payload: update_data["cavali_data"] = payload["cavali_results"]
        elif "drive_folder_url" in payload: update_data["drive_data"] = {"drive_folder_url": payload["drive_folder_url"]}
        stmt = pg_insert(models.OperationStaging).values(tracking_id=tracking_id, **update_data).on_conflict_do_update(index_elements=['tracking_id'], set_=update_data)
        db.execute(stmt)
        staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).with_for_update().first()
        if staging_record and staging_record.parsed_data and staging_record.cavali_data and staging_record.drive_data:
            print(f"AGGREGATOR: Todos los datos para {tracking_id} recibidos. Procesando.")
            final_payload = staging_record.initial_payload; final_payload.update({ "parsed_results": staging_record.parsed_data, "cavali_results": staging_record.cavali_data, "drive_folder_url": staging_record.drive_data["drive_folder_url"] })
            process_final_operation(final_payload, db); db.delete(staging_record)
        db.commit()
    except Exception as e:
        db.rollback(); traceback.print_exc(); raise
    return Response(status_code=status.HTTP_204_NO_CONTENT)

def process_final_operation(payload: dict, db: Session):
    repo = OperationRepository(db)
    invoices_by_currency = defaultdict(list)
    for inv in payload["parsed_results"]: invoices_by_currency[inv['currency']].append(inv)
    original_tracking_id = payload["tracking_id"]

    for currency, invoices_in_group in invoices_by_currency.items():
        operation_id = repo.generar_siguiente_id_operacion() # This generates OP-fecha-incremental
        print(f"FINALIZER: Generado nuevo ID {operation_id} para moneda {currency}.")
        repo.save_full_operation(
            operation_id=operation_id, metadata=payload['metadata'], drive_url=payload['drive_folder_url'],
            invoices_data=invoices_in_group, cavali_results_map=payload['cavali_results']
        )
        print(f"FINALIZER: Operación {operation_id} guardada en DB.")

        notification_payload = {
            "operation_id": operation_id, 
            "user_email": payload.get("user_email"), 
            "metadata": payload.get("metadata"),
            "drive_folder_url": payload.get("drive_folder_url"), 
            "cavali_results": payload.get("cavali_results"),
            "parsed_results": invoices_in_group, 
            "gcs_paths": payload.get("gcs_paths"),
            "original_tracking_id": original_tracking_id
        }

        trello_update_data = {}
        try:
            if TRELLO_SERVICE_URL:
                print(f"FINALIZER: Llamando directamente a Trello para op {operation_id}")
                trello_response = requests.post(f"{TRELLO_SERVICE_URL}/create-card", json=notification_payload, timeout=300)
                trello_response.raise_for_status()
                trello_result = trello_response.json()
                trello_update_data = {"trello_data": {"created": True, "card_id": trello_result.get("card_id")}}
            else:
                print("ADVERTENCIA: TRELLO_SERVICE_URL no está configurada.")
                trello_update_data = {"trello_data": {"created": False, "error": "TRELLO_SERVICE_URL no configurada"}}
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falló la llamada a Trello para op {operation_id}. Error: {e}")
            trello_update_data = {"trello_data": {"created": False, "error": str(e)}}
        finally:
            stmt = pg_insert(models.OperationStaging).values(
                tracking_id=original_tracking_id, **trello_update_data
            ).on_conflict_do_update(index_elements=['tracking_id'], set_=trello_update_data)
            db.execute(stmt)
            db.commit()


        publisher.publish(TOPIC_OPERATION_PERSISTED, json.dumps(notification_payload).encode("utf-8")).result()
        print(f"FINALIZER: Notificación para Gmail (Op: {operation_id}) publicada.")

# ==============================================================================
# ROL 3: ENDPOINTS DE CONSULTA
# ==============================================================================
@app.get("/operation-status/{tracking_id}")
async def get_operation_status(tracking_id: str, db: Session = Depends(get_db)):
    """
    Consulta la tabla de staging y devuelve el progreso, incluyendo la URL de Drive.
    """
    staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).first()
    if not staging_record:
        raise HTTPException(status_code=404, detail="Operación no encontrada en la etapa de procesamiento.")

    drive_info = staging_record.drive_data or {}
    return {
        "drive_folder_url": drive_info.get("drive_folder_url")
    }

@app.get("/api/operaciones")
async def get_user_operations(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    repo = OperationRepository(db); last_login = repo.update_and_get_last_login(user['email'], user.get('name', ''))
    return {"last_login": last_login.isoformat() if last_login else None, "operations": repo.get_operations_by_user_email(user['email'])}

@app.get("/api/gestiones/operaciones")
async def get_operaciones_gestion(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Endpoint REFACTORIZADO para el panel de Gestión.
    Aplica lógica de negocio para priorizar y cargar eficientemente los datos.
    """
    # 1. Definir un orden de prioridad: las más antiguas y de mayor monto primero.
    # Usamos case para dar más peso a la antigüedad.
    priority_order = case(
        (models.Operacion.fecha_creacion < (datetime.now() - timedelta(days=5)), 1),
        (models.Operacion.fecha_creacion < (datetime.now() - timedelta(days=2)), 2),
        else_=3
    ).asc()

    # 2. Carga optimizada (Eager Loading) para evitar múltiples consultas a la BD
    base_query = db.query(models.Operacion).options(
        joinedload(models.Operacion.cliente),
        selectinload(models.Operacion.facturas).joinedload(models.Factura.deudor),
        selectinload(models.Operacion.gestiones).joinedload(models.Gestion.analista)
    )

    # 3. Filtro por rol
    if user['email'] == "kevin.tupac@capitalexpress.cl":
        query = base_query.filter(models.Operacion.estado.notin_(['Completada', 'Rechazada', 'Anulada']))
    else:
        # Un analista estándar solo ve lo que está activamente en verificación
        query = base_query.filter(models.Operacion.estado == 'En Verificación')

    operaciones_db = query.order_by(priority_order, models.Operacion.monto_sumatoria_total.desc()).all()

    # 4. Transformación de datos: el backend hace los cálculos, no el frontend
    resultado_formateado = []
    for op in operaciones_db:
        # Lógica de negocio para Alertas de IA (ejemplo)
        alerta_ia = None
        antiquity_days = (datetime.now().date() - op.fecha_creacion.date()).days
        if antiquity_days > 3 and len(op.gestiones) == 0:
            alerta_ia = {"tipo": "llamar", "texto": "¡Llamar ya! Operación con más de 3 días sin gestión."}
        
        resultado_formateado.append({
            "id": op.id,
            "cliente": op.cliente.razon_social if op.cliente else "N/A",
            "deudor": op.facturas[0].deudor.razon_social if op.facturas and op.facturas[0].deudor else "N/A",
            "montoTotal": op.monto_sumatoria_total,
            "moneda": op.moneda_sumatoria,
            "fechaIngreso": op.fecha_creacion.isoformat(),
            "antiquity": antiquity_days,
            "correosEnviados": 2, # Este dato debería venir de una tabla de logs de correo
            "adelantoExpress": op.adelanto_express, # Corregir el campo en el modelo si es necesario
            "estadoOperacion": op.estado,
            "gestiones": [{
                "fecha": g.fecha_creacion.isoformat(),
                "tipo": g.tipo,
                "resultado": g.resultado,
                "notas": g.notas,
                "analista": g.analista.nombre if g.analista else "Sistema"
            } for g in op.gestiones],
            "facturas": [{
                "folio": f.numero_documento,
                "monto": f.monto_total,
                "moneda": f.moneda,
                "estado": f.estado
            } for f in op.facturas],
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

    nueva_gestion = models.Gestion(
        id_operacion=op_id,
        analista_email=user['email'],
        **gestion_data.dict()
    )
    db.add(nueva_gestion)
    db.commit()
    db.refresh(nueva_gestion)
    return nueva_gestion


# orquestador-service-0/main.py
class FacturaUpdate(BaseModel):
    estado: str # "Verificada" o "Rechazada"

@app.patch("/api/operaciones/{op_id}/facturas/{folio}")
async def actualizar_factura_y_operacion(op_id: str, folio: str, update_data: FacturaUpdate, db: Session = Depends(get_db)):
    factura = db.query(models.Factura).filter(
        models.Factura.id_operacion == op_id, models.Factura.numero_documento == folio
    ).first()
    if not factura:
        raise HTTPException(status_code=404, detail="Factura no encontrada")

    factura.estado = update_data.estado
    
    # --- Lógica de Negocio Centralizada ---
    operacion = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
    facturas_de_operacion = operacion.facturas
    
    if any(f.estado == 'Rechazada' for f in facturas_de_operacion):
        operacion.estado = 'Discrepancia'
    elif all(f.estado == 'Verificada' for f in facturas_de_operacion):
        operacion.estado = 'Conforme'
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
    nueva_gestion = models.Gestion(
        id_operacion=op_id,
        analista_email=user['email'],
        tipo="Adelanto Express",
        resultado="Movido a cola de Adelanto",
        notas=data.justificacion
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
    
    # Cambia el estado a "Completada" para que se filtre automáticamente
    operacion.estado = 'Completada'
    db.commit()
    return {"status": "ok", "message": f"Operación {op_id} marcada como completada."}