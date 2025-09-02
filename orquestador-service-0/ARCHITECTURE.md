# Arquitectura Modular del Orquestador

## Resumen
El orquestador ha sido refactorizado de un archivo monolítico de +700 líneas a una arquitectura modular siguiendo el patrón de separación de responsabilidades.

## Estructura

```
orquestador-service-0/
├── main.py                     # FastAPI app + routing (70 líneas)
├── core/                       # Utilities centralizadas
│   ├── config.py              # Variables de entorno
│   └── dependencies.py        # Auth & DB dependencies
├── services/                   # Lógica de negocio
│   ├── operation_service.py   # Procesamiento de operaciones
│   ├── microservice_client.py # HTTP clients
│   └── notification_service.py # Gmail/Trello
├── routers/                    # Endpoints por dominio
│   ├── operations.py          # /operations/*
│   ├── dashboard.py           # /api/operaciones/*
│   ├── gestiones.py           # /api/gestiones/*
│   └── users.py               # /api/users/*
├── database.py                 # (sin cambios)
├── repository.py              # (sin cambios)
├── models.py                  # (sin cambios)
└── main_legacy.py             # Respaldo del original
```

## Flujo de Procesamiento

### Nuevo flujo (secuencial con Drive paralelo):
1. **Frontend** → `/operations/submit` 
2. **Orquestador** → `operation_service.submit_operation()`
3. **Parser** → HTTP directo (síncrono)
4. **Cavali** → HTTP directo (síncrono, con tolerancia a fallos)
5. **Drive** → Pub/sub paralelo
6. **Aggregator** → Espera Drive, finaliza operación
7. **Notificaciones** → Gmail/Trello directos

### Beneficios:
- **Control total**: Flujo secuencial controlado
- **Tolerancia a fallos**: Cavali puede fallar sin afectar el proceso
- **Performance**: Drive en paralelo
- **Simplificado**: Menos pub/sub, más directo

## Endpoints

### Principales:
- `POST /operations/submit` - Envío de operaciones
- `GET /operations/status/{id}` - Estado de operación
- `POST /operations/pubsub-aggregator` - Aggregator de Drive

### Legacy (compatibilidad):
- `POST /submit-operation` → redirige a `/operations/submit`
- `GET /operation-status/{id}` → redirige a `/operations/status/{id}`

### API Dashboard:
- `GET /api/operaciones` - Lista de operaciones
- `GET /api/operaciones/{id}/detalle` - Detalle completo

### API Gestión:
- `GET /api/gestiones/operaciones` - Cola de gestión
- `POST /api/operaciones/{id}/gestiones` - Nueva gestión
- `POST /api/operaciones/{id}/adelanto-express` - Adelanto express

## Responsabilidades

### `main.py` (70 líneas)
- Setup de FastAPI
- CORS middleware  
- Include routers
- Health check
- Endpoints legacy (compatibilidad)

### `services/operation_service.py`
- Procesamiento completo de operaciones
- Orquestación Parser → Cavali → Drive
- Manejo del staging y agregación
- Finalización y persistencia

### `services/microservice_client.py`
- HTTP clients para todos los microservicios
- Manejo de timeouts y errores
- Reutilizable desde cualquier servicio

### `services/notification_service.py`
- Envío de notificaciones Gmail/Trello
- Formateo de payloads
- Manejo de fallos de notificación

### `routers/`
- **operations.py**: Endpoints de operaciones
- **dashboard.py**: APIs del dashboard
- **gestiones.py**: APIs de gestión
- **users.py**: APIs de usuarios

### `core/`
- **config.py**: Configuración centralizada
- **dependencies.py**: Auth Firebase y DB

## Deployment

Sin cambios en el deployment. La nueva arquitectura mantiene la misma interfaz externa:

```bash
docker build -t orquestador-service:latest .
docker push [registry]/orquestador-service:latest
# Deploy normal...
```

## Rollback

Si hay problemas, rollback disponible:

```bash
cp main_legacy.py main.py
# Rebuild y deploy
```

## Próximos Pasos

1. **Eliminar main_legacy.py** después de verificar estabilidad
2. **Tests unitarios** para cada servicio
3. **Métricas y logging** mejorados  
4. **Separar servicios** en microservicios independientes (futuro)

## Migration Log

- ✅ Refactorización completada
- ✅ Pub/sub subscriptions limpiadas
- ✅ Código duplicado eliminado
- ✅ Arquitectura modular implementada
- ✅ Retrocompatibilidad mantenida