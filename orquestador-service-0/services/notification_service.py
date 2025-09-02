import logging
from typing import Dict
from services.microservice_client import microservice_client

class NotificationService:
    """Servicio para manejo de notificaciones (Gmail, Trello)"""
    
    def send_notifications(self, payload: Dict) -> bool:
        """Envía notificaciones a Gmail y Trello"""
        gmail_success = self.send_gmail_notification(payload)
        trello_success = self.send_trello_notification(payload)
        
        return gmail_success or trello_success  # Al menos una debe funcionar
    
    def send_gmail_notification(self, payload: Dict) -> bool:
        """Envía notificación por Gmail"""
        try:
            operation_id = payload["operation_id"]
            metadata = payload["metadata"]
            invoices_data = payload["invoices_data"]
            
            # Preparar datos para Gmail
            gmail_payload = {
                "operation_id": operation_id,
                "user_email": metadata.get("user_email", "unknown"),
                "invoice_count": len(invoices_data),
                "total_amount": sum(float(inv.get("total_amount", 0)) for inv in invoices_data),
                "currency": invoices_data[0].get("currency") if invoices_data else "PEN"
            }
            
            success = microservice_client.call_gmail_service(gmail_payload)
            if success:
                logging.info(f"NOTIFICATION: Gmail enviado para {operation_id}")
            else:
                logging.warning(f"NOTIFICATION: Gmail falló para {operation_id}")
                
            return success
            
        except Exception as e:
            logging.error(f"NOTIFICATION: Error enviando Gmail: {e}")
            return False
    
    def send_trello_notification(self, payload: Dict) -> bool:
        """Envía notificación a Trello"""
        try:
            operation_id = payload["operation_id"]
            metadata = payload["metadata"]
            invoices_data = payload["invoices_data"]
            
            # Preparar datos para Trello
            trello_payload = {
                "operation_id": operation_id,
                "client_name": invoices_data[0].get("client_name") if invoices_data else "Cliente desconocido",
                "invoice_count": len(invoices_data),
                "total_amount": sum(float(inv.get("total_amount", 0)) for inv in invoices_data),
                "currency": invoices_data[0].get("currency") if invoices_data else "PEN",
                "user_email": metadata.get("user_email", "unknown")
            }
            
            success = microservice_client.call_trello_service(trello_payload)
            if success:
                logging.info(f"NOTIFICATION: Trello card creada para {operation_id}")
            else:
                logging.warning(f"NOTIFICATION: Trello falló para {operation_id}")
                
            return success
            
        except Exception as e:
            logging.error(f"NOTIFICATION: Error enviando Trello: {e}")
            return False

# Singleton instance
notification_service = NotificationService()