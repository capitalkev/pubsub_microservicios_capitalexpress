from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from core.dependencies import get_current_user
from database import get_db
from repository import OperationRepository
from services.microservice_client import microservice_client
import models

router = APIRouter(prefix="/api", tags=["gestiones"])

class GestionCreate(BaseModel):
    tipo: str
    descripcion: str

class AdelantoExpressRequest(BaseModel):
    justificacion: str

class SendVerificationRequest(BaseModel):
    emails: List[str]
    comentario: Optional[str] = None

@router.get("/gestiones/operaciones")
async def get_gestiones_operations(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtiene operaciones para la cola de gestión"""
    try:
        repo = OperationRepository(db)
        user_email = user['email']
        user_role = 'admin'  # TODO: Implement role detection
        
        operations = repo.get_gestiones_operations(user_email, user_role)
        
        operations_data = []
        for op in operations:
            operations_data.append({
                "id": op.id,
                "fechaCreacion": op.fecha_creacion.isoformat(),
                "cliente": op.cliente.razon_social if op.cliente else "Cliente desconocido",
                "monto": op.monto_sumatoria_total,
                "moneda": op.moneda_sumatoria,
                "estado": op.estado,
                "facturas": len(op.facturas),
                "gestiones": len(op.gestiones),
                "analistaAsignado": op.analista_asignado.nombre if op.analista_asignado else None,
                "urlCarpetaDrive": op.url_carpeta_drive,
                "adelantoExpress": op.adelanto_express,
                "solicitaAdelanto": op.solicita_adelanto,
                "porcentajeAdelanto": op.porcentaje_adelanto
            })
        
        return {"operations": operations_data}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/operaciones/{op_id}/gestiones", status_code=status.HTTP_201_CREATED)
async def create_gestion(
    op_id: str,
    gestion_data: GestionCreate,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Crea una nueva gestión para una operación"""
    try:
        new_gestion = models.Gestion(
            id_operacion=op_id,
            tipo=gestion_data.tipo,
            descripcion=gestion_data.descripcion,
            creado_por=user['name'],
            analista_email=user['email']
        )
        
        db.add(new_gestion)
        db.commit()
        db.refresh(new_gestion)
        
        return {"message": "Gestión creada exitosamente", "gestion_id": new_gestion.id}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/gestiones/{gestion_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_gestion(
    gestion_id: int,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Elimina una gestión"""
    try:
        gestion = db.query(models.Gestion).filter(models.Gestion.id == gestion_id).first()
        if not gestion:
            raise HTTPException(status_code=404, detail="Gestión no encontrada")
        
        db.delete(gestion)
        db.commit()
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/operaciones/{op_id}/adelanto-express", status_code=status.HTTP_200_OK)
async def set_adelanto_express(
    op_id: str,
    request: AdelantoExpressRequest,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Marca una operación como adelanto express"""
    try:
        operation = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        operation.adelanto_express = True
        
        # Crear gestión
        new_gestion = models.Gestion(
            id_operacion=op_id,
            tipo="Adelanto Express",
            descripcion=f"Marcado como adelanto express. Justificación: {request.justificacion}",
            creado_por=user['name'],
            analista_email=user['email']
        )
        
        db.add(new_gestion)
        db.commit()
        
        return {"message": "Operación marcada como adelanto express exitosamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/operaciones/{op_id}/send-verification", status_code=status.HTTP_200_OK)
async def send_verification_email(
    op_id: str,
    request: SendVerificationRequest,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Envía email de verificación"""
    try:
        operation = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        # Preparar payload para Gmail
        gmail_payload = {
            "operation_id": op_id,
            "recipients": request.emails,
            "subject": f"Verificación requerida - Operación {op_id}",
            "message": request.comentario or "Se requiere verificación para esta operación.",
            "sender": user['email'],
            "verification": True
        }
        
        # Enviar email
        success = microservice_client.call_gmail_service(gmail_payload)
        if not success:
            raise HTTPException(status_code=500, detail="Error enviando email de verificación")
        
        # Crear gestión
        emails_str = ", ".join(request.emails)
        new_gestion = models.Gestion(
            id_operacion=op_id,
            tipo="Verificación Enviada",
            descripcion=f"Email de verificación enviado a: {emails_str}. Comentario: {request.comentario or 'N/A'}",
            creado_por=user['name'],
            analista_email=user['email']
        )
        
        db.add(new_gestion)
        db.commit()
        
        return {"message": "Email de verificación enviado exitosamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))