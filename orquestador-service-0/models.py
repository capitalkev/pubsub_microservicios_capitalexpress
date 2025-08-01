# app/infrastructure/persistence/models.py
from sqlalchemy import Column, String, Float, ForeignKey, Integer, DateTime, Text, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
from sqlalchemy.dialects.postgresql import JSONB

class Empresa(Base):
    __tablename__ = "empresas"
    ruc = Column(String(15), primary_key=True, index=True)
    razon_social = Column(String(255))
    operaciones = relationship("Operacion", back_populates="cliente")
    
class Operacion(Base):
    __tablename__ = "operaciones"
    id = Column(String(255), primary_key=True)
    cliente_ruc = Column(String(15), ForeignKey("empresas.ruc"), nullable=False)
    email_usuario = Column(String(255))
    nombre_ejecutivo = Column(Text)
    url_carpeta_drive = Column(Text)
    monto_sumatoria_total = Column(Float, server_default='0')
    moneda_sumatoria = Column(String(10))
    fecha_creacion = Column(DateTime(timezone=True), server_default=func.now())
    tasa_operacion = Column(Float, nullable=True)
    comision = Column(Float, nullable=True)
    solicita_adelanto = Column(Boolean, default=False)
    porcentaje_adelanto = Column(Float, default=0)
    desembolso_banco = Column(String(100), nullable=True)
    desembolso_tipo = Column(String(50), nullable=True)
    desembolso_moneda = Column(String(10), nullable=True)
    desembolso_numero = Column(String(100), nullable=True)
    estado = Column(String(50), default='En Verificación', nullable=False)
    adelanto_express = Column(Boolean, default=False, nullable=False)
    analista_asignado_email = Column(String(255), ForeignKey("usuarios.email"), nullable=True)

    cliente = relationship("Empresa", back_populates="operaciones")
    gestiones = relationship("Gestion", back_populates="operacion")
    facturas = relationship("Factura", back_populates="operacion")
    analista_asignado = relationship("Usuario")

class Factura(Base):
    __tablename__ = "facturas"
    id = Column(Integer, primary_key=True)
    id_operacion = Column(String(255), ForeignKey("operaciones.id"), nullable=False)
    numero_documento = Column(String(255), nullable=False, index=True)
    deudor_ruc = Column(String(15), ForeignKey("empresas.ruc"), nullable=False)
    fecha_emision = Column(DateTime(timezone=True))
    fecha_vencimiento = Column(DateTime(timezone=True))
    moneda = Column(String(10))
    monto_total = Column(Float)
    monto_neto = Column(Float)
    mensaje_cavali = Column(Text)
    id_proceso_cavali = Column(String(255))
    estado = Column(String(50), default='Pendiente', nullable=False)
    
    operacion = relationship("Operacion", back_populates="facturas")
    deudor = relationship("Empresa")
    

class Usuario(Base):
    __tablename__ = "usuarios"
    email = Column(String(255), primary_key=True, index=True)
    nombre = Column(String(255))
    ultimo_ingreso = Column(DateTime(timezone=True), server_default=func.now())
    rol = Column(String(50), nullable=False, default='ventas')
    
class OperationStaging(Base):
    __tablename__ = "operations_staging"
    tracking_id = Column(String(255), primary_key=True, index=True)
    initial_payload = Column(JSONB)
    parsed_data = Column(JSONB, nullable=True)
    cavali_data = Column(JSONB, nullable=True)
    drive_data = Column(JSONB, nullable=True)
    trello_data = Column(JSONB, nullable=True)
    fecha_creacion = Column(DateTime(timezone=True), server_default=func.now())
    
class Gestion(Base):
    __tablename__ = "gestiones"
    id = Column(Integer, primary_key=True)
    id_operacion = Column(String(255), ForeignKey("operaciones.id"), nullable=False)
    fecha_creacion = Column(DateTime(timezone=True), server_default=func.now())
    analista_email = Column(String(255), ForeignKey("usuarios.email"))
    tipo = Column(String(50)) # Ej: "Llamada", "Email Manual", "Adelanto Express"
    resultado = Column(String(100)) # Ej: "Conforme", "No Contesta"
    nombre_contacto = Column(String(255), nullable=True)
    cargo_contacto = Column(String(100), nullable=True)
    telefono_email_contacto = Column(String(255), nullable=True)
    notas = Column(Text, nullable=True)
    operacion = relationship("Operacion", back_populates="gestiones")
    
    analista = relationship("Usuario")

