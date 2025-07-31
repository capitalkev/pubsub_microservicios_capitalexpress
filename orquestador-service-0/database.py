# orquestador-service-0/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# --- Importaciones añadidas ---
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector


load_dotenv()

connector = Connector()

def get_db_connection():
    """
    Crea y retorna un motor de conexión a la base de datos de Cloud SQL.
    """
    instance_connection_name = os.getenv("DB_INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")

    if not instance_connection_name:
        raise ValueError("La variable de entorno DB_INSTANCE_CONNECTION_NAME no está definida.")
    if not db_user:
        raise ValueError("La variable de entorno DB_USER no está definida.")

    engine = create_engine(
        "postgresql+pg8000://",
        creator=lambda: connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name
        ),
        pool_pre_ping=True,
    )
    return engine

engine = get_db_connection()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """
    Abre y cierra una sesión de base de datos por cada petición.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()