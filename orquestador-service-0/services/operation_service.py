import json
import logging
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.cloud import storage, pubsub_v1

from core.config import config
from repository import OperationRepository
import models

class OperationService:
    """Servicio para procesamiento de operaciones"""
    
    def __init__(self):
        self.storage_client = storage.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.bucket = self.storage_client.bucket(config.BUCKET_NAME)
        
        # Topics
        self.TOPIC_OPERATION_SUBMITTED = self.publisher.topic_path(config.GCP_PROJECT_ID, "operation-submitted")
        self.TOPIC_OPERATION_PERSISTED = self.publisher.topic_path(config.GCP_PROJECT_ID, "operation-persisted")
    
    def upload_file(self, file, upload_folder: str, subfolder: str) -> str:
        """Sube un archivo a GCS y retorna la ruta"""
        blob_path = f"{upload_folder}/{subfolder}/{file.filename}"
        blob = self.bucket.blob(blob_path)
        blob.upload_from_file(file.file)
        return f"gs://{config.BUCKET_NAME}/{blob_path}"
    
    async def submit_operation(self, metadata: dict, xml_files: List, pdf_files: List, respaldo_files: List, user: dict, db: Session) -> Dict:
        """Procesa una nueva operación"""
        try:
            tracking_id = str(uuid.uuid4())
            upload_folder = f"operations/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/{tracking_id}"
            
            # Upload files to GCS
            gcs_paths = {
                "xml": [self.upload_file(f, upload_folder, "xml") for f in xml_files],
                "pdf": [self.upload_file(f, upload_folder, "pdf") for f in pdf_files],
                "respaldo": [self.upload_file(f, upload_folder, "respaldos") for f in respaldo_files]
            }
            
            operation_data = {
                "tracking_id": tracking_id,
                "user_email": user['email'],
                "metadata": metadata,
                "gcs_paths": gcs_paths
            }
            
            # Procesar directamente Parser y Cavali, Drive en paralelo
            await self.process_operation_sync(operation_data, db)
            return {"status": "processing", "tracking_id": tracking_id}
            
        except Exception as e:
            logging.error(f"OPERATION: Error procesando operación: {e}")
            raise e
    
    async def process_operation_sync(self, operation_data: dict, db: Session):
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
            
            # Import services locally to avoid circular imports
            from services.microservice_client import microservice_client
            
            # 1. Llamar Parser directamente
            parsed_results = await microservice_client.call_parser_service(operation_data)
            if not parsed_results:
                logging.error(f"SYNC: Parser falló para {tracking_id}")
                raise Exception("Parser service failed")
            
            # 2. Llamar Cavali directamente (con tolerancia a fallos)
            cavali_results = await microservice_client.call_cavali_service(operation_data)
            if not cavali_results:
                logging.warning(f"SYNC: Cavali falló para {tracking_id}, continuando sin validación")
                cavali_results = {}
            
            # 3. Publicar a Drive en paralelo
            drive_payload = {**operation_data}
            self.publisher.publish(self.TOPIC_OPERATION_SUBMITTED, json.dumps(drive_payload).encode("utf-8")).result()
            
            # 4. Esperar resultado de Drive y finalizar
            await self.wait_for_drive_and_finalize(tracking_id, operation_data, parsed_results, cavali_results, db)
            
        except Exception as e:
            logging.error(f"SYNC: Error procesando {operation_data.get('tracking_id')}: {e}")
            raise
    
    async def wait_for_drive_and_finalize(self, tracking_id: str, operation_data: dict, parsed_results: dict, cavali_results: dict, db: Session):
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
    
    def process_aggregated_data(self, tracking_id: str, drive_folder_url: str, db: Session):
        """Procesa los datos cuando Drive completa (llamado por aggregator)"""
        try:
            # Obtener datos del staging
            staging_record = db.query(models.OperationStaging).filter_by(tracking_id=tracking_id).with_for_update().first()
            
            if not staging_record or not staging_record.parsed_data:
                logging.error(f"AGGREGATOR: No se encontraron datos de staging para {tracking_id}")
                return False
            
            # Preparar payload final
            final_payload = staging_record.initial_payload
            final_payload.update({
                "parsed_results": staging_record.parsed_data,
                "cavali_results": staging_record.cavali_data or {},
                "drive_folder_url": drive_folder_url
            })
            
            # Procesar operación final
            self.process_final_operation(final_payload, db)
            
            # Limpiar staging
            db.delete(staging_record)
            db.commit()
            
            logging.info(f"AGGREGATOR: Operación {tracking_id} procesada y finalizada exitosamente")
            return True
            
        except Exception as e:
            logging.error(f"AGGREGATOR: Error procesando datos agregados para {tracking_id}: {e}")
            db.rollback()
            return False
    
    def process_final_operation(self, payload: dict, db: Session):
        """Procesa la operación final y envía notificaciones"""
        try:
            repo = OperationRepository(db)
            original_tracking_id = payload["tracking_id"]
            
            # Generar ID de operación y guardar en BD
            operation_id = repo.generar_siguiente_id_operacion()
            metadata = payload["metadata"]
            drive_url = payload["drive_folder_url"]
            invoices_data = payload["parsed_results"]
            cavali_results_map = payload["cavali_results"]
            
            repo.save_full_operation(operation_id, metadata, drive_url, invoices_data, cavali_results_map)
            
            logging.info(f"FINALIZER: Operación {original_tracking_id} guardada como {operation_id}")
            
            # Enviar notificaciones
            notification_payload = {
                "tracking_id": original_tracking_id,
                "operation_id": operation_id,
                "metadata": metadata,
                "invoices_data": invoices_data
            }
            
            # Llamadas directas a servicios de notificación
            from services.notification_service import notification_service
            notification_service.send_notifications(notification_payload)
            
            # También enviar por pub/sub para compatibilidad
            self.publisher.publish(self.TOPIC_OPERATION_PERSISTED, json.dumps(notification_payload).encode("utf-8")).result()
            
        except Exception as e:
            logging.error(f"FINALIZER: Error procesando operación final: {e}")
            raise

# Singleton instance
operation_service = OperationService()