# orquestador-service-0/repository.py (Versión Final Correcta)
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import case, func
from datetime import datetime, timedelta, timezone
from models import Gestion, Operacion, Factura, Empresa, Usuario 
from sqlalchemy import text
from sqlalchemy.orm import joinedload, selectinload

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
        
        self.db.execute(text("LOCK TABLE operaciones IN EXCLUSIVE MODE"))
        
        last_id_today = self.db.query(func.max(Operacion.id)).filter(Operacion.id.like(f"{id_prefix}%")).scalar()
        
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
    
    def get_dashboard_operations(self, user_email: str, user_role: str, offset: int = 0, limit: int = 20) -> Dict[str, Any]:
        """
        Obtiene operaciones para el dashboard principal con paginación.
        - Admins ven todas las operaciones.
        - Ventas ven solo las operaciones creadas por ellos.
        """

        base_query = self.db.query(
            Operacion.id, Operacion.fecha_creacion.label("fechaIngreso"),
            Empresa.razon_social.label("cliente"), Operacion.monto_sumatoria_total.label("monto"),
            Operacion.moneda_sumatoria.label("moneda"),
            Operacion.estado
        ).join(Empresa, Operacion.cliente_ruc == Empresa.ruc)

        count_query = self.db.query(func.count(Operacion.id))

        if user_role != 'admin':
            query = base_query.filter(Operacion.email_usuario == user_email)
            count_query = count_query.filter(Operacion.email_usuario == user_email)
        else:
            query = base_query

        # Contar el total de operaciones para la paginación
        total_records = count_query.scalar()

        # Aplicar orden, paginación y ejecutar la consulta
        results = query.order_by(Operacion.fecha_creacion.desc()).offset(offset).limit(limit).all()

        operations_list = [
            {
                "id": r.id,
                "fechaIngreso": r.fechaIngreso.isoformat(),
                "cliente": r.cliente, "monto": r.monto,
                "moneda": r.moneda,
                "estado": r.estado
            } for r in results
        ]

        return {"operations": operations_list, "total": total_records}
    
    
    def get_gestiones_operations(self, user_email: str, user_role: str) -> List[Operacion]:
        """
        Obtiene las operaciones para la cola de tareas de gestión con una lógica de roles robusta.
        - Admins ven todas las operaciones activas.
        - Gestión ve solo las operaciones activas asignadas a ellos.
        """

        ESTADOS_DE_GESTION_ACTIVA = ['En Verificación', 'Discrepancia', 'Conforme', 'Adelanto']

        base_query = self.db.query(Operacion).options(
            joinedload(Operacion.cliente),
            selectinload(Operacion.facturas).joinedload(Factura.deudor),
            selectinload(Operacion.gestiones).joinedload(Gestion.analista),
            joinedload(Operacion.analista_asignado)
        ).filter(Operacion.estado.in_(ESTADOS_DE_GESTION_ACTIVA))

        if user_role != 'admin':
            query = base_query.filter(Operacion.analista_asignado_email == user_email)
        else:
            query = base_query

        priority_order = case(
            (Operacion.fecha_creacion < (datetime.now(timezone.utc) - timedelta(days=5)), 1),
            (Operacion.fecha_creacion < (datetime.now(timezone.utc) - timedelta(days=2)), 2),
            else_=3
        ).asc()

        return query.order_by(priority_order, Operacion.monto_sumatoria_total.desc()).all()
        
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
    
    def check_duplicate_invoices(self, invoices_data: List[Dict]) -> Dict[str, Any]:
        """
        Verifica si alguna factura ya existe basándose en:
        - RUC deudor + número documento + monto + fecha emisión
        """
        duplicates = []
        new_invoices = []
        
        for inv in invoices_data:
            # Crear fingerprint de la factura
            debtor_ruc = inv.get('debtor_ruc')
            document_id = inv.get('document_id') 
            total_amount = float(inv.get('total_amount', 0))
            issue_date = inv.get('issue_date')
            
            if not all([debtor_ruc, document_id, issue_date]):
                continue  # Skip facturas con datos incompletos
                
            # Buscar factura existente con mismo fingerprint
            existing = self.db.query(Factura).filter(
                Factura.deudor_ruc == debtor_ruc,
                Factura.numero_documento == document_id,
                Factura.monto_total == total_amount,
                func.date(Factura.fecha_emision) == datetime.fromisoformat(issue_date).date()
            ).first()
            
            if existing:
                duplicates.append({
                    'invoice': inv,
                    'existing_operation': existing.id_operacion,
                    'fingerprint': f"{debtor_ruc}-{document_id}-{total_amount}-{issue_date}"
                })
            else:
                new_invoices.append(inv)
        
        return {
            'duplicates': duplicates,
            'new_invoices': new_invoices,
            'has_duplicates': len(duplicates) > 0
        }