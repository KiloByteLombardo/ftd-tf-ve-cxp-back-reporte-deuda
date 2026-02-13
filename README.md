# Deuda VZLA API

API para el procesamiento y generación de reportes de deuda de órdenes de compra para **Farmatodo Venezuela**. Lee archivos Excel de órdenes de compra, aplica reglas de negocio (filtrado, conversión de moneda, enriquecimiento de datos) y genera reportes de deuda real en USD, subiéndolos a BigQuery y Google Cloud Storage.

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Tecnologías](#tecnologías)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Requisitos Previos](#requisitos-previos)
- [Variables de Entorno](#variables-de-entorno)
- [Instalación y Ejecución](#instalación-y-ejecución)
- [API Endpoints](#api-endpoints)
- [Flujo de Procesamiento](#flujo-de-procesamiento)
- [Despliegue en Cloud Run](#despliegue-en-cloud-run)

---

## Arquitectura

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Frontend   │────▶│   Flask API      │────▶│   BigQuery       │
│   (Cliente)  │◀────│   (gunicorn)     │────▶│   Cloud Storage  │
└──────────────┘ SSE └──────────────────┘     └──────────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │ Google Sheets│
                     │ (áreas)      │
                     └──────────────┘
```

## Tecnologías

| Categoría | Tecnología |
|---|---|
| Runtime | Python 3.11 |
| Framework Web | Flask 3.0.0 |
| Servidor WSGI | Gunicorn 21.2.0 |
| Procesamiento de datos | Pandas 2.1.4 |
| Lectura/escritura Excel | openpyxl 3.1.2, xlrd 2.0.1 |
| BigQuery | google-cloud-bigquery 3.13.0 |
| Cloud Storage | google-cloud-storage 2.14.0 |
| Google Sheets | gspread 5.12.0 |
| Contenedores | Docker |

## Estructura del Proyecto

```
deuda_vzla/
├── src/
│   ├── __init__.py
│   ├── api.py              # Endpoints de la API Flask
│   └── venezuela.py         # Lógica de negocio y procesamiento
├── .env                     # Variables de entorno (no versionado)
├── credentials.json         # Credenciales GCP (no versionado)
├── docker-compose.yml       # Configuración Docker para desarrollo local
├── Dockerfile               # Imagen Docker de la aplicación
├── deploy.ps1               # Script de despliegue a Cloud Run (PowerShell)
├── requirements.txt         # Dependencias de Python
├── SSE_FRONTEND_GUIDE.md    # Guía de integración SSE para frontend
└── README.md
```

## Requisitos Previos

- **Python 3.11+**
- **Docker** y **Docker Compose** (para ejecución local con contenedores)
- **Google Cloud SDK** (`gcloud`) para despliegue
- Archivo `credentials.json` con las credenciales de una cuenta de servicio de GCP con permisos para:
  - BigQuery (lectura/escritura)
  - Cloud Storage (lectura/escritura)
  - Google Sheets (lectura)

## Variables de Entorno

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

| Variable | Descripción | Ejemplo |
|---|---|---|
| `GCP_PROJECT_ID` | ID del proyecto en GCP | `gtf-cxp` |
| `GCP_REGION` | Región de GCP | `us-central1` |
| `BQ_DATASET_ID` | Dataset de BigQuery para resultados | `cxp_vzla` |
| `BQ_TABLE_ID` | Tabla de BigQuery para resultados | `vzla_servicios_deuda` |
| `GCS_BUCKET_NAME` | Bucket de Cloud Storage para archivos Excel | `mi-bucket` |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | ID del spreadsheet con mapeo solicitante → área | `abc123...` |
| `GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL` | Email de la cuenta de servicio | `sa@project.iam.gserviceaccount.com` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Ruta al archivo de credenciales (solo local) | `./credentials.json` |
| `PORT` | Puerto del servidor | `8080` |
| `DEBUG` | Modo debug | `false` |

> **Nota:** En Cloud Run, la autenticación se maneja mediante la cuenta de servicio asociada al servicio. No se necesita `GOOGLE_APPLICATION_CREDENTIALS`.

## Instalación y Ejecución

### Desarrollo Local (Docker Compose)

```bash
# Clonar el repositorio
git clone <url-del-repositorio>
cd deuda_vzla

# Asegurar que existen .env y credentials.json en la raíz

# Levantar el servicio
docker-compose up --build
```

La API estará disponible en `http://localhost:8080`.

### Desarrollo Local (sin Docker)

```bash
# Crear entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
python -m src.api
```

## API Endpoints

### Salud y Diagnóstico

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/health` | Health check. Retorna estado del servicio. |
| `GET` | `/test/bigquery` | Verifica conexión a BigQuery. |
| `GET` | `/test/storage` | Verifica conexión a Cloud Storage. |
| `GET` | `/test/sheets` | Verifica configuración de Google Sheets. |

### Generación de Deuda

| Método | Endpoint | Datos | Descripción |
|---|---|---|---|
| `POST` | `/generar-deuda` | `ordenes_compra` (Excel), `tasa` (Excel) | Genera reporte de deuda con tasas desde archivo Excel. |
| `POST` | `/generar-deuda/stream` | `ordenes_compra` (Excel), `tasa` (Excel) | Igual que el anterior, con progreso en tiempo real vía **SSE**. |
| `POST` | `/generar-deuda-bq` | `ordenes_compra` (Excel) | Genera reporte de deuda con tasas desde BigQuery (`cxp_vzla.bcv_tasas`). |
| `POST` | `/generar-deuda-bq/stream` | `ordenes_compra` (Excel) | Igual que el anterior, con progreso en tiempo real vía **SSE**. |

### Historial

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/archivos` | Lista archivos procesados en Cloud Storage. Acepta `?limit=N`. |

### Ejemplo de uso con `curl`

```bash
# Generar deuda con tasas desde archivo
curl -X POST http://localhost:8080/generar-deuda \
  -F "ordenes_compra=@ordenes.xlsx" \
  -F "tasa=@tasas.xlsx"

# Generar deuda con tasas desde BigQuery
curl -X POST http://localhost:8080/generar-deuda-bq \
  -F "ordenes_compra=@ordenes.xlsx"
```

### Server-Sent Events (SSE)

Los endpoints `/stream` envían eventos en tiempo real con el progreso del procesamiento:

| Evento | Descripción |
|---|---|
| `progress` | `{type, step, percent}` — Progreso parcial (5%, 15%, ..., 100%) |
| `complete` | `{type, status, message, filas_procesadas, archivo, url_cloud_storage, percent}` — Resultado final |
| `error` | `{type, message}` — Error durante el procesamiento |

> Consultar `SSE_FRONTEND_GUIDE.md` para la guía completa de integración desde el frontend.

## Flujo de Procesamiento

1. **Lectura de archivos** — Se reciben los Excel de órdenes de compra (y opcionalmente tasas de cambio).
2. **Obtención de tasas** — Desde el archivo Excel subido o desde BigQuery (`cxp_vzla.bcv_tasas`).
3. **Filtrado** — Se excluyen órdenes con `ESTADO_CIERRE == "CERRADO"`.
4. **Enriquecimiento** — Se calcula el año fiscal desde `FECHA_ORDEN` y se obtiene el `AREA` del solicitante desde Google Sheets.
5. **Conversión de moneda** — Se cruzan tasas de cambio por fecha y moneda (VES/USD, COP/USD, EUR/USD).
6. **Cálculo de deuda** — Se computan:
   - `MONTO OC` y `MONTO OC USD`
   - `MONTO OC ASOCIADO` y `MONTO OC ASOCIADO USD`
   - `MONTO REAL DEUDA` = `MONTO OC USD` − `MONTO OC ASOCIADO USD`
7. **Generación de Excel** — Se crea el archivo de salida con formato.
8. **Carga a la nube** — Se sube el resultado a BigQuery (modo `WRITE_APPEND`) y a Cloud Storage bajo `vzla/{YYYY-MM-DD_HH-MM-SS}/`.

## Despliegue en Cloud Run

El proyecto incluye un script de PowerShell para despliegue automatizado:

```powershell
.\deploy.ps1
```

### Configuración por defecto del despliegue

| Parámetro | Valor |
|---|---|
| Proyecto GCP | `gtf-cxp` |
| Región | `us-central1` |
| Servicio | `servicios-deuda-vzla` |
| Memoria | 1Gi |
| CPU | 1 |
| Timeout | 300s |
| Concurrencia | 80 |

### Pasos del despliegue

1. Construye la imagen Docker.
2. Sube la imagen a Google Container Registry (`gcr.io/gtf-cxp/servicios-deuda-vzla:latest`).
3. Despliega en Cloud Run con las variables de entorno del `.env`.
4. Habilita las APIs necesarias: Cloud Build, Cloud Run, BigQuery, Cloud Storage, Google Sheets.

---

## Almacenamiento en Cloud Storage

Los archivos generados se almacenan con la siguiente estructura:

```
vzla/
└── {YYYY-MM-DD_HH-MM-SS}/
    └── resultado_deuda_{D_M_Y}.xlsx
```

La zona horaria utilizada es **America/Caracas**.
