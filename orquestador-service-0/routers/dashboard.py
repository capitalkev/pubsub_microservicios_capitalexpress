from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from core.dependencies import get_current_user
from database import get_db
from repository import OperationRepository
import models

router = APIRouter(prefix="/api", tags=["dashboard"])

@router.get("/operaciones")
async def get_operations(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    estado: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtiene operaciones para el dashboard principal"""
    try:
        repo = OperationRepository(db)
        user_email = user['email']
        user_role = 'admin'
        
        offset = (page - 1) * limit
        result = repo.get_dashboard_operations(user_email, user_role, offset, limit, estado)
        
        total_pages = (result["total"] + limit - 1) // limit
        
        return {
            "operations": result["operations"],
            "pagination": {
                "current_page": page,
                "total_pages": total_pages,
                "total_records": result["total"],
                "page_size": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/operaciones/{op_id}/detalle")
async def get_operation_detail(
    op_id: str,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtiene detalle completo de una operación"""
    try:
        operation = db.query(models.Operacion).filter(models.Operacion.id == op_id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        # Get related data
        client = db.query(models.Empresa).filter(models.Empresa.ruc == operation.cliente_ruc).first()
        invoices = db.query(models.Factura).filter(models.Factura.id_operacion == op_id).all()
        gestiones = db.query(models.Gestion).filter(models.Gestion.id_operacion == op_id).all()
        
        # Build response
        operation_data = {
            "id": operation.id,
            "fechaCreacion": operation.fecha_creacion.isoformat(),
            "estado": operation.estado,
            "clienteRuc": operation.cliente_ruc,
            "clienteNombre": client.razon_social if client else "Cliente desconocido",
            "emailUsuario": operation.email_usuario,
            "nombreEjecutivo": operation.nombre_ejecutivo,
            "montoSumatoria": operation.monto_sumatoria_total,
            "monedaSumatoria": operation.moneda_sumatoria,
            "tasaOperacion": operation.tasa_operacion,
            "comision": operation.comision,
            "solicitaAdelanto": operation.solicita_adelanto,
            "porcentajeAdelanto": operation.porcentaje_adelanto,
            "adelantoExpress": operation.adelanto_express,
            "urlCarpetaDrive": operation.url_carpeta_drive,
            "desembolso": {
                "banco": operation.desembolso_banco,
                "tipo": operation.desembolso_tipo,
                "moneda": operation.desembolso_moneda,
                "numero": operation.desembolso_numero
            },
            "facturas": [
                {
                    "id": factura.id,
                    "numeroDocumento": factura.numero_documento,
                    "deudorRuc": factura.deudor_ruc,
                    "fechaEmision": factura.fecha_emision.isoformat() if factura.fecha_emision else None,
                    "fechaVencimiento": factura.fecha_vencimiento.isoformat() if factura.fecha_vencimiento else None,
                    "moneda": factura.moneda,
                    "montoTotal": factura.monto_total,
                    "montoNeto": factura.monto_neto,
                    "estado": factura.estado,
                    "mensajeCavali": factura.mensaje_cavali,
                    "idProcesoCavali": factura.id_proceso_cavali
                } for factura in invoices
            ],
            "gestiones": [
                {
                    "id": gestion.id,
                    "tipo": gestion.tipo,
                    "descripcion": gestion.descripcion,
                    "fechaCreacion": gestion.fecha_creacion.isoformat(),
                    "creadoPor": gestion.creado_por,
                    "analistaEmail": gestion.analista_email
                } for gestion in gestiones
            ]
        }
        
        return operation_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))