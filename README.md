# Backend FastAPI – Customer Analytics sobre IBM i

Este backend ya no depende del archivo `VENTASPF.xlsx`.
Ahora consume directamente la tabla `GLEARN211/VENTASPF` en PUB400.COM y genera la tabla analítica `GLEARN211/SALESTRANS` dentro del mismo IBM i.

## 1) Variables de entorno

Cree un archivo `.env` con esta base:

```env
JT400_JAR=backend/drivers/jt400.jar
IBMI_DEFAULT_HOST=PUB400.COM
IBMI_USER=GLEARN21
IBMI_PASSWORD=SU_PASSWORD
IBMI_JDBC_URL=jdbc:as400://PUB400.COM;prompt=false;naming=system;errors=full;date format=iso;time format=iso
IBMI_LIBRARY=GLEARN211
IBMI_SOURCE_TABLE=VENTASPF
IBMI_TARGET_TABLE=SALESTRANS
```

## 2) Instalar dependencias

```bash
pip install -r requirements.txt
```

## 3) Crear y cargar SALESTRANS desde VENTASPF

```bash
python -m scripts.sync_salestrans_from_ibmi
```

También puede hacerlo vía API:

```bash
POST /api/admin/refresh-salestrans
```

## 4) Levantar la API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Endpoints

- `GET /health`
- `POST /api/admin/refresh-salestrans`
- `GET /api/filter-options`
- `GET /api/kpis?zone=All&category=All&year=2025`
- `GET /api/monthly-sales?zone=All&category=All&year=2025`
- `GET /api/top-products?zone=All&category=All&year=2025&limit=8`
- `GET /api/revenue-by-zone`
- `GET /api/revenue-by-category`
- `GET /api/cohorts`
- `GET /api/journey`

## Notas técnicas

- `VENTASPF` es la fuente operacional.
- `SALESTRANS` es la tabla de trabajo analítica para el dashboard.
- El backend conserva el mismo contrato JSON para no romper React.
