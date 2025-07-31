from google.cloud import storage
from google.oauth2.credentials import Credentials

def download_blob_as_bytes(gs_path: str, creds: Credentials) -> bytes:
    """
    Descarga un archivo de GCS usando las credenciales de usuario ya obtenidas.
    """
    path_parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name, blob_path = path_parts[0], path_parts[1]
    client = storage.Client(credentials=creds)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()