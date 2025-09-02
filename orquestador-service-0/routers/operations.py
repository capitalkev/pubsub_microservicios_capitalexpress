from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Request, Response, status
from typing import List, Annotated
from sqlalchemy.orm import Session
import json
import base64

from core.dependencies import get_current_user
from database import get_db
from services.operation_service import operation_service
import models

router = APIRouter(prefix="/operations", tags=["operations"])

@router.post("/submit", status_code=status.HTTP_202_ACCEPTED)
async def submit_operation(
    metadata_str: Annotated[str, Form(alias="metadata")],
    xml_files: Annotated[List[UploadFile], File(alias="xml_files")],
    pdf_files: Annotated[List[UploadFile], File(alias="pdf_files")],
    respaldo_files: Annotated[List[UploadFile], File(alias="respaldo_files")],
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Envía una nueva operación para procesamiento"""
    try:
        metadata = json.loads(metadata_str)
        return await operation_service.submit_operation(metadata, xml_files, pdf_files, respaldo_files, user, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pubsub-aggregator", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_aggregator(request: Request, db: Session = Depends(get_db)):
    """Endpoint para recibir datos de Drive via Pub/Sub"""
    try:
        body = await request.json()
        message = body.get("message", {})
        if not message or "data" not in message:
            raise HTTPException(status_code=400)

        payload = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
        tracking_id = payload["tracking_id"]
        
        # Solo manejamos Drive ahora, Parser y Cavali se procesan síncronamente
        if "drive_folder_url" in payload:
            drive_folder_url = payload["drive_folder_url"]
            success = operation_service.process_aggregated_data(tracking_id, drive_folder_url, db)
            if not success:
                return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        return Response(status_code=status.HTTP_204_NO_CONTENT)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{tracking_id}")
async def get_operation_status(tracking_id: str, db: Session = Depends(get_db)):
    """Obtiene el estado de una operación por tracking ID"""
    staging = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).first()
    if staging:
        return {
            "status": "processing",
            "tracking_id": tracking_id,
            "has_parsed_data": staging.parsed_data is not None,
            "has_cavali_data": staging.cavali_data is not None,
            "has_drive_data": staging.drive_data is not None
        }
    
    # Buscar en operaciones finalizadas
    operation = db.query(models.Operacion).filter(models.Operacion.id.contains(tracking_id)).first()
    if operation:
        return {
            "status": "completed",
            "tracking_id": tracking_id,
            "operation_id": operation.id,
            "estado": operation.estado
        }
    
    return {"status": "not_found", "tracking_id": tracking_id}