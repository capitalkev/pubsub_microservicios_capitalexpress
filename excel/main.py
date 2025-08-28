# gcp-microservicios/excel/main.py
import gspread
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

class Contacto(BaseModel):
    ruc: str
    correo: str
    nombre_deudor: Optional[str] = None

app = FastAPI(title="Microservicio de Google Sheets (Versión Robusta)")

try:
    credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "operaciones-peru-ef02622bfc3d.json")
    gc = gspread.service_account(filename=credentials_file)
    sh = gc.open("Contactos verificaciones")
    worksheet = sh.worksheet("CORREOS")
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar Google Sheets: {e}")

@app.post("/update-contact", summary="Actualizar o Crear Contacto")
def update_contact(contacto: Contacto):
    try:
        all_rucs = worksheet.col_values(1)
        if contacto.ruc in all_rucs:
            fila_num = all_rucs.index(contacto.ruc) + 1
            correos_actuales_str = worksheet.cell(fila_num, 3).value or ""
            correo_nuevo = contacto.correo.strip()
            if not correo_nuevo:
                return {"status": "SUCCESS", "message": "RUC encontrado, sin correo nuevo para añadir."}
            lista_de_correos = {c.strip() for c in correos_actuales_str.split(';') if c.strip()}
            if correo_nuevo in lista_de_correos:
                return {"status": "SUCCESS", "message": f"El correo '{correo_nuevo}' ya existía."}
            lista_de_correos.add(correo_nuevo)
            worksheet.update_cell(fila_num, 3, ";".join(sorted(lista_de_correos)))
            return {"status": "SUCCESS", "message": f"Contacto para RUC {contacto.ruc} actualizado."}
        else:
            if not contacto.nombre_deudor:
                raise HTTPException(status_code=400, detail=f"RUC '{contacto.ruc}' no existe y se necesita 'nombre_deudor' para crearlo.")
            worksheet.append_row([contacto.ruc, contacto.nombre_deudor, contacto.correo.strip()])
            return {"status": "CREATED", "message": f"Nuevo contacto para RUC {contacto.ruc} creado."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado en excel-service: {str(e)}")

@app.get("/get-emails/{ruc}", summary="Obtener correos de un RUC")
def get_emails(ruc: str):
    # Verifica que la cella de RUC no esté vacía y si existe en la hoja se envia el correo
    try:
        celda = worksheet.find(ruc, in_column=1)
        correos = ""
        if celda:
            correos_val = worksheet.cell(celda.row, 3).value
            if correos_val:
                correos = correos_val
        return {"ruc": ruc, "emails": correos}
    except gspread.exceptions.CellNotFound:
        return {"ruc": ruc, "emails": ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado al obtener correos: {str(e)}")