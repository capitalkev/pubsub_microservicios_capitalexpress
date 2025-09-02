import requests
import logging
import asyncio
from typing import Dict, Optional
from core.config import config

class MicroserviceClient:
    """Cliente HTTP para comunicación con microservicios"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
    
    async def call_parser_service(self, operation_data: dict) -> dict:
        """Llama al parser service directamente"""
        try:
            if not config.PARSER_SERVICE_URL:
                logging.error("PARSER_SERVICE_URL no configurada")
                return {}
                
            url = f"{config.PARSER_SERVICE_URL}/parse-direct"
            
            # Usar asyncio.wait_for en lugar de asyncio.timeout para compatibilidad con Python < 3.11
            async def make_request():
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, lambda: self.session.post(url, json=operation_data, timeout=300))
                response.raise_for_status()
                return response.json()
            
            result = await asyncio.wait_for(make_request(), timeout=300)
            logging.info(f"PARSER: Éxito para {operation_data['tracking_id']}")
            return result.get("parsed_results", {})
                
        except Exception as e:
            logging.error(f"PARSER: Error para {operation_data['tracking_id']}: {e}")
            return {}

    async def call_cavali_service(self, operation_data: dict) -> dict:
        """Llama al cavali service directamente con tolerancia a fallos"""
        try:
            if not config.CAVALI_SERVICE_URL:
                logging.warning("CAVALI_SERVICE_URL no configurada, continuando sin validación")
                return {}
                
            url = f"{config.CAVALI_SERVICE_URL}/validate-direct"
            
            # Usar asyncio.wait_for en lugar de asyncio.timeout para compatibilidad con Python < 3.11
            async def make_request():
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, lambda: self.session.post(url, json=operation_data, timeout=600))
                response.raise_for_status()
                return response.json()
            
            result = await asyncio.wait_for(make_request(), timeout=600)
            logging.info(f"CAVALI: Éxito para {operation_data['tracking_id']}")
            return result.get("cavali_results", {})
                
        except Exception as e:
            logging.warning(f"CAVALI: Error para {operation_data['tracking_id']}: {e}, continuando sin validación")
            return {}
    
    def call_gmail_service(self, payload: dict) -> bool:
        """Llama al servicio de Gmail"""
        try:
            if not config.GMAIL_SERVICE_URL:
                logging.warning("GMAIL_SERVICE_URL no configurada")
                return False
                
            url = f"{config.GMAIL_SERVICE_URL}/send-email"
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logging.info(f"GMAIL: Email enviado exitosamente")
            return True
            
        except Exception as e:
            logging.error(f"GMAIL: Error enviando email: {e}")
            return False
    
    async def call_drive_service(self, operation_data: dict) -> dict:
        """Llama al drive service directamente con tolerancia a fallos"""
        try:
            if not config.DRIVE_SERVICE_URL:
                logging.warning("DRIVE_SERVICE_URL no configurada, continuando sin archivado")
                return {}
                
            url = f"{config.DRIVE_SERVICE_URL}/archive-direct"
            
            # Usar asyncio.wait_for en lugar de asyncio.timeout para compatibilidad con Python < 3.11
            async def make_request():
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, lambda: self.session.post(url, json=operation_data, timeout=30))
                response.raise_for_status()
                return response.json()
            
            result = await asyncio.wait_for(make_request(), timeout=30)
            logging.info(f"DRIVE: Éxito para {operation_data['tracking_id']}")
            return result
                
        except Exception as e:
            logging.warning(f"DRIVE: Error para {operation_data['tracking_id']}: {e}, continuando sin archivado")
            return {}
    
    def call_trello_service(self, payload: dict) -> bool:
        """Llama al servicio de Trello"""
        try:
            if not config.TRELLO_SERVICE_URL:
                logging.warning("TRELLO_SERVICE_URL no configurada")
                return False
                
            url = f"{config.TRELLO_SERVICE_URL}/create-card"
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logging.info(f"TRELLO: Card creada exitosamente")
            return True
            
        except Exception as e:
            logging.error(f"TRELLO: Error creando card: {e}")
            return False

# Singleton instance
microservice_client = MicroserviceClient()