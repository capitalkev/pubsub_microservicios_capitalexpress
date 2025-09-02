import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # GCP Configuration
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    # Microservice URLs
    TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL")
    DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL")
    GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL")
    PARSER_SERVICE_URL = os.getenv("PARSER_SERVICE_URL")
    CAVALI_SERVICE_URL = os.getenv("CAVALI_SERVICE_URL")
    
    # Database Configuration
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")
    DB_INSTANCE_CONNECTION_NAME = os.getenv("DB_INSTANCE_CONNECTION_NAME")

config = Config()