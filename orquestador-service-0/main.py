import uuid
import json
import os
import base64
import traceback
import requests
from typing import List, Annotated, Optional
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
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
    if not firebase_admin._apps:
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
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL")
GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL")

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

        # Se asigna el rol como un string simple desde la BD
        decoded_token['role'] = user_in_db.rol 
        return decoded_token
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido o error de base de datos: {e}")

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
        tracking_id = str(uuid.uuid4())
        upload_folder = f"operations/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/{tracking_id}"
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
        operation_id = repo.generar_siguiente_id_operacion()
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

        try:
            if GMAIL_SERVICE_URL:
                print(f"FINALIZER: Llamando directamente a Gmail para op {operation_id}")
                gmail_response = requests.post(f"{GMAIL_SERVICE_URL}/send-email", json=notification_payload, timeout=300)
                gmail_response.raise_for_status()
                print(f"FINALIZER: Llamada a Gmail para op {operation_id} completada con éxito.")
            else:
                print("ADVERTENCIA: GMAIL_SERVICE_URL no está configurada.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falló la llamada a Gmail para op {operation_id}. Error: {e}")
            if e.response:
                print(f"GMAIL-SERVICE respondió con: {e.response.text}")
        
        try:
            if TRELLO_SERVICE_URL:
                print(f"FINALIZER: Llamando directamente a Trello para op {operation_id}")
                trello_response = requests.post(f"{TRELLO_SERVICE_URL}/create-card", json=notification_payload, timeout=300)
                trello_response.raise_for_status()
            else:
                print("ADVERTENCIA: TRELLO_SERVICE_URL no está configurada.")
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falló la llamada a Trello para op {operation_id}. Error: {e}")

        

        publisher.publish(TOPIC_OPERATION_PERSISTED, json.dumps(notification_payload).encode("utf-8")).result()
        print(f"FINALIZER: Notificación para Gmail (Op: {operation_id}) publicada.")

# ==============================================================================
# ROL 3: ENDPOINTS DE CONSULTA
# ==============================================================================
@app.get("/operation-status/{tracking_id}")
async def get_operation_status(tracking_id: str, db: Session = Depends(get_db)):
    staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).first()
    if not staging_record:
        raise HTTPException(status_code=404, detail="Operación no encontrada en la etapa de procesamiento.")
    drive_info = staging_record.drive_data or {}
    return {"drive_folder_url": drive_info.get("drive_folder_url")}

@app.get("/api/operaciones")
async def get_user_operations(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    repo = OperationRepository(db)
    last_login = repo.update_and_get_last_login(user['email'], user.get('name', ''))
    
    user_role = user.get('role')
    operations = repo.get_dashboard_operations(user['email'], user_role)
    
    return {"last_login": last_login.isoformat() if last_login else None, "operations": operations}


@app.get("/api/gestiones/operaciones")
async def get_operaciones_gestion(user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    # La lógica compleja ahora está en el repositorio. El endpoint es simple y claro.
    repo = OperationRepository(db)
    
    # 1. Obtenemos las operaciones usando el nuevo método centralizado.
    operaciones_db = repo.get_gestiones_operations(user_email=user['email'], user_role=user.get('role'))
    
    # 2. El resto de la función es solo para formatear la respuesta, como debe ser.
    resultado_formateado = []
    for op in operaciones_db:
        alerta_ia = None
        antiquity_days = (datetime.now(timezone.utc).date() - op.fecha_creacion.date()).days
        if antiquity_days > 3 and len(op.gestiones) == 0:
            alerta_ia = {"tipo": "llamar", "texto": "¡Llamar ya! Operación con más de 3 días sin gestión."}

        # Aseguramos que el estado de la operación se mapea correctamente para el frontend
        estado_operacion = op.estado
        if op.estado == 'Conforme' and not op.adelanto_express:
             # Este es el estado que el frontend entiende para mostrar el botón "Completar"
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
            "analistaAsignado": { "nombre": op.analista_asignado.nombre if op.analista_asignado else "Sin Asignar", "email": op.analista_asignado.email if op.analista_asignado else None },
            "gestiones": [{ "fecha": g.fecha_creacion.isoformat(), "tipo": g.tipo, "resultado": g.resultado, "notas": g.notas, "analista": g.analista.nombre if g.analista else "Sistema" } for g in op.gestiones],
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
    # La dependencia 'get_current_user' ya ha hecho todo el trabajo pesado:
    # 1. Verificó el token.
    # 2. Consultó la base de datos para obtener el rol.
    # 3. Lo añadió al diccionario 'user'.
    
    # Solo necesitamos buscar de nuevo para obtener el objeto completo para la respuesta.
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