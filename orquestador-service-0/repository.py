# orquestador-service-0/repository.py (Versión Final Correcta)
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import func
from datetime import datetime, timezone
from models import Operacion, Factura, Empresa, Usuario 
from sqlalchemy import text # <-- Asegúrate de tener esta importación

class OperationRepository:
    def __init__(self, db: Session):
        self.db = db

    def _find_or_create_company(self, ruc: str, name: str) -> Optional[Empresa]:
        if not ruc or not name: return None
        empresa = self.db.query(Empresa).filter(Empresa.ruc == ruc).first()
        if not empresa:
            empresa = Empresa(ruc=ruc, razon_social=name)
            self.db.add(empresa)
            self.db.flush()
        return empresa

    def generar_siguiente_id_operacion(self) -> str:
        """
        Genera un ID de operación secuencial de forma segura, usando un bloqueo
        de transacción para prevenir condiciones de carrera.
        """
        today_str = datetime.now(timezone.utc).strftime('%Y%m%d')
        id_prefix = f"OP-{today_str}-"
        
        # --- LÓGICA CORREGIDA Y A PRUEBA DE FALLOS (SIN CAMBIAR LA DB) ---
        # Esta línea bloquea la tabla temporalmente para que solo una instancia
        # pueda calcular el siguiente ID a la vez. Es una operación atómica.
        self.db.execute(text("LOCK TABLE operaciones IN EXCLUSIVE MODE"))
        
        last_id_today = self.db.query(func.max(Operacion.id)).filter(Operacion.id.like(f"{id_prefix}%")).scalar()
        # --- FIN DE LA CORRECCIÓN ---
        
        next_number = 1
        if last_id_today:
            try:
                next_number = int(last_id_today.split('-')[-1]) + 1
            except (ValueError, IndexError):
                next_number = 1
                
        return f"{id_prefix}{next_number:03d}"

    def save_full_operation(self, operation_id: str, metadata: dict, drive_url: str, invoices_data: List[Dict], cavali_results_map: Dict) -> str: 
        if not invoices_data:
            raise ValueError("No se puede guardar una operación sin datos de facturas.")

        client_ruc = invoices_data[0].get('client_ruc')
        client_name = invoices_data[0].get('client_name')
        primer_cliente = self._find_or_create_company(client_ruc, client_name)

        monto_sumatoria = sum(float(inv.get('total_amount', 0)) for inv in invoices_data)
        moneda_operacion = invoices_data[0].get('currency')

        email = metadata.get('user_email', 'unknown@example.com')
        tasaOperacion = metadata.get('tasaOperacion')
        comision = metadata.get('comision')
        solicitudAdelanto_obj = metadata.get('solicitudAdelanto', {})
        solicitaAdelanto_bool = solicitudAdelanto_obj.get('solicita', False)
        porcentajeAdelanto_float = solicitudAdelanto_obj.get('porcentaje', 0)
        cuentas_desembolso_data = metadata.get('cuentasDesembolso', [])
        cuenta_principal = cuentas_desembolso_data[0] if cuentas_desembolso_data else {}
        nombre_ejecutivo = email.split('@')[0].replace('.', ' ').title()
        
        db_operacion = Operacion(
            id=operation_id,
            cliente_ruc=primer_cliente.ruc,
            email_usuario=email,
            nombre_ejecutivo=nombre_ejecutivo,
            url_carpeta_drive=drive_url,
            monto_sumatoria_total=monto_sumatoria,
            moneda_sumatoria=moneda_operacion,
            tasa_operacion=tasaOperacion,
            comision=comision,
            solicita_adelanto=solicitaAdelanto_bool,
            porcentaje_adelanto=porcentajeAdelanto_float,
            desembolso_banco = cuenta_principal.get('banco'),
            desembolso_tipo = cuenta_principal.get('tipo'),
            desembolso_moneda = cuenta_principal.get('moneda'),
            desembolso_numero = cuenta_principal.get('numero')
        )
        self.db.add(db_operacion)
        
        for inv in invoices_data:
            deudor = self._find_or_create_company(inv.get('debtor_ruc'), inv.get('debtor_name'))
            cavali_data = cavali_results_map.get(inv.get('xml_filename'), {})
            
            db_factura = Factura(
                id_operacion=operation_id,
                numero_documento=inv.get('document_id'),
                deudor_ruc=deudor.ruc if deudor else None,
                fecha_emision=datetime.fromisoformat(inv.get('issue_date')) if inv.get('issue_date') else None,
                fecha_vencimiento=datetime.fromisoformat(inv.get('due_date')) if inv.get('due_date') else None,
                moneda=inv.get('currency'),
                monto_total=float(inv.get('total_amount')),
                monto_neto=float(inv.get('net_amount')),
                mensaje_cavali= cavali_data.get("message"),
                id_proceso_cavali=cavali_data.get("process_id") 
            )
            self.db.add(db_factura)
            
        self.db.commit()
        return operation_id
    
    def get_operations_by_user_email(self, email: str) -> List[Dict[str, Any]]:
        if email == "kevin.tupac@capitalexpress.cl":
            results = (
                self.db.query(
                    Operacion.id, Operacion.fecha_creacion.label("fechaIngreso"),
                    Empresa.razon_social.label("cliente"), Operacion.monto_sumatoria_total.label("monto"),
                    Operacion.moneda_sumatoria.label("moneda")
                ).join(Empresa, Operacion.cliente_ruc == Empresa.ruc).order_by(Operacion.fecha_creacion.desc()).all()
            )
        else:
            results = (
                self.db.query(
                    Operacion.id, Operacion.fecha_creacion.label("fechaIngreso"),
                    Empresa.razon_social.label("cliente"), Operacion.monto_sumatoria_total.label("monto"),
                    Operacion.moneda_sumatoria.label("moneda")
                ).join(Empresa, Operacion.cliente_ruc == Empresa.ruc).filter(Operacion.email_usuario == email).order_by(Operacion.fecha_creacion.desc()).all()
            )
        return [{"id": r.id, "fechaIngreso": r.fechaIngreso.isoformat(), "cliente": r.cliente, "monto": r.monto, "moneda": r.moneda, "estado": "En Verificación"} for r in results]
        
    def update_and_get_last_login(self, email: str, name: str) -> Optional[datetime]:
        now = datetime.now(timezone.utc)
        usuario = self.db.query(Usuario).filter(Usuario.email == email).first()
        if usuario:
            previous_login = usuario.ultimo_ingreso
            usuario.ultimo_ingreso = now
        else:
            previous_login = None
            usuario = Usuario(email=email, nombre=name, ultimo_ingreso=now)
            self.db.add(usuario)
        self.db.commit()
        return previous_login