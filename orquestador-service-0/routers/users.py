from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel

from core.dependencies import get_current_user
from database import get_db
from repository import OperationRepository
import models

router = APIRouter(prefix="/api/users", tags=["users"])

class UserSession(BaseModel):
    uid: str
    email: str
    name: str
    lastLogin: str = None

@router.get("/me", response_model=UserSession)
async def get_current_user_info(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtiene información del usuario actual"""
    try:
        repo = OperationRepository(db)
        last_login = repo.update_and_get_last_login(user['email'], user['name'])
        
        return UserSession(
            uid=user['uid'],
            email=user['email'],
            name=user['name'],
            lastLogin=last_login.isoformat() if last_login else None
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysts")
async def get_analysts(
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtiene lista de analistas disponibles"""
    try:
        # TODO: Implement proper role-based filtering
        usuarios = db.query(models.Usuario).all()
        
        analysts = [
            {
                "email": usuario.email,
                "nombre": usuario.nombre,
                "ultimo_ingreso": usuario.ultimo_ingreso.isoformat() if usuario.ultimo_ingreso else None
            }
            for usuario in usuarios
        ]
        
        return {"analysts": analysts}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))