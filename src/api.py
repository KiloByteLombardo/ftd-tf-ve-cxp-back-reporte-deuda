"""
API endpoints para el sistema de procesamiento de deuda Venezuela
"""
from flask import Flask, jsonify, request, send_file, Response, stream_with_context
from flask_cors import CORS
from google.cloud import bigquery
from google.cloud import storage
import os
import json
import tempfile
from datetime import datetime
from werkzeug.utils import secure_filename
from src.venezuela import procesar_archivos

app = Flask(__name__)
# Configurar CORS para permitir todas las solicitudes
CORS(app, resources={r"/*": {"origins": "*"}})

# Configurar logging para mostrar requests HTTP
import logging

# Configurar el logger de werkzeug para mostrar requests
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.INFO)
app.logger.setLevel(logging.INFO)


def get_bigquery_client():
    """Obtiene un cliente de BigQuery usando ADC (Application Default Credentials)"""
    try:
        client = bigquery.Client()
        return client
    except Exception as e:
        print(f"✗ Error al crear cliente de BigQuery: {e}")
        raise


def get_storage_client():
    """Obtiene un cliente de Cloud Storage usando ADC (Application Default Credentials)"""
    try:
        client = storage.Client()
        return client
    except Exception as e:
        print(f"✗ Error al crear cliente de Cloud Storage: {e}")
        raise


@app.route('/health', methods=['GET'])
def health():
    """
    Endpoint de health check
    """
    print(f"Health check - {request.method} {request.path}")
    return jsonify({
        'status': 'healthy',
        'service': 'deuda_vzla_api'
    }), 200


@app.route('/test/bigquery', methods=['GET'])
def test_bigquery():
    """
    Endpoint para probar la conexión a BigQuery
    """
    print(f"Test BigQuery - {request.method} {request.path}")
    try:
        project_id = os.getenv('GCP_PROJECT_ID')
        if not project_id:
            print("✗ GCP_PROJECT_ID no está configurado")
            return jsonify({
                'status': 'error',
                'message': 'GCP_PROJECT_ID no está configurado en las variables de entorno'
            }), 500
        
        print(f"Probando conexión a BigQuery con proyecto: {project_id}")
        client = get_bigquery_client()
        
        # Intentar listar datasets para verificar la conexión
        datasets = list(client.list_datasets())
        print(f"✓ Conexión a BigQuery exitosa. Datasets encontrados: {len(datasets)}")
        
        return jsonify({
            'status': 'success',
            'message': 'Conexión a BigQuery exitosa',
            'project_id': project_id,
            'datasets_count': len(datasets),
            'datasets': [dataset.dataset_id for dataset in datasets[:10]]  # Primeros 10
        }), 200
        
    except Exception as e:
        print(f"✗ Error al conectar con BigQuery: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error al conectar con BigQuery: {str(e)}'
        }), 500


@app.route('/test/storage', methods=['GET'])
def test_storage():
    """
    Endpoint para probar la conexión a Cloud Storage
    """
    print(f"Test Storage - {request.method} {request.path}")
    try:
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        project_id = os.getenv('GCP_PROJECT_ID')
        
        print(f"Probando conexión a Cloud Storage")
        client = get_storage_client()
        
        # Intentar listar buckets para verificar la conexión
        buckets = list(client.list_buckets())
        print(f"✓ Conexión a Cloud Storage exitosa. Buckets encontrados: {len(buckets)}")
        
        response = {
            'status': 'success',
            'message': 'Conexión a Cloud Storage exitosa',
            'buckets_count': len(buckets),
            'buckets': [bucket.name for bucket in buckets[:10]]  # Primeros 10
        }
        
        if project_id:
            response['project_id'] = project_id
        if bucket_name:
            response['configured_bucket'] = bucket_name
            # Verificar si el bucket configurado existe
            try:
                bucket = client.bucket(bucket_name)
                response['bucket_exists'] = bucket.exists()
                print(f"Bucket configurado '{bucket_name}' existe: {response['bucket_exists']}")
            except Exception as e:
                response['bucket_check_error'] = str(e)
                print(f"⚠ Error al verificar bucket: {e}")
        
        return jsonify(response), 200
        
    except Exception as e:
        print(f"✗ Error al conectar con Cloud Storage: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error al conectar con Cloud Storage: {str(e)}'
        }), 500


@app.route('/test/sheets', methods=['GET'])
def test_sheets():
    """
    Endpoint para probar la conexión a Google Sheets
    """
    print(f"Test Sheets - {request.method} {request.path}")
    try:
        # Google Sheets se accede típicamente a través de la API de Google Sheets
        # Para verificar la conexión, necesitaríamos usar gspread o google-api-python-client
        # Por ahora, solo verificamos que las credenciales estén configuradas
        
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        service_account_email = os.getenv('GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL')
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
        
        print(f"Verificando configuración de Google Sheets...")
        
        if not credentials_path and not service_account_email:
            print("⚠ Credenciales de Google Sheets no configuradas completamente")
            return jsonify({
                'status': 'warning',
                'message': 'Credenciales de Google Sheets no configuradas completamente',
                'note': 'Se requiere GOOGLE_APPLICATION_CREDENTIALS o GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL'
            }), 200
        
        print(f"✓ Configuración de Google Sheets detectada")
        return jsonify({
            'status': 'success',
            'message': 'Configuración de Google Sheets detectada',
            'has_credentials': bool(credentials_path),
            'service_account_email': service_account_email if service_account_email else 'No configurado',
            'spreadsheet_id': spreadsheet_id if spreadsheet_id else 'No configurado'
        }), 200
        
    except Exception as e:
        print(f"✗ Error al verificar Google Sheets: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error al verificar Google Sheets: {str(e)}'
        }), 500


@app.route('/generar-deuda', methods=['POST'])
def generar_deuda():
    """
    Endpoint para generar el archivo de deuda procesado.
    Recibe dos archivos Excel: Ordenes de Compra y Tasa.
    
    FormData:
        - ordenes_compra: Archivo Excel de Ordenes de Compra
        - tasa: Archivo Excel de Tasa
    """
    print(f"Generar Deuda - {request.method} {request.path}")
    
    # Verificar que se hayan enviado los archivos
    if 'ordenes_compra' not in request.files:
        print("✗ Error: No se encontró el archivo 'ordenes_compra'")
        return jsonify({
            'status': 'error',
            'message': 'Falta el archivo de Ordenes de Compra. Use el campo "ordenes_compra"'
        }), 400
    
    if 'tasa' not in request.files:
        print("✗ Error: No se encontró el archivo 'tasa'")
        return jsonify({
            'status': 'error',
            'message': 'Falta el archivo de Tasa. Use el campo "tasa"'
        }), 400
    
    ordenes_file = request.files['ordenes_compra']
    tasa_file = request.files['tasa']
    
    # Verificar que los archivos no estén vacíos
    if ordenes_file.filename == '':
        print("✗ Error: El archivo de Ordenes de Compra está vacío")
        return jsonify({
            'status': 'error',
            'message': 'El archivo de Ordenes de Compra está vacío'
        }), 400
    
    if tasa_file.filename == '':
        print("✗ Error: El archivo de Tasa está vacío")
        return jsonify({
            'status': 'error',
            'message': 'El archivo de Tasa está vacío'
        }), 400
    
    # Crear directorio temporal para los archivos
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Guardar archivos temporalmente
        ordenes_filename = secure_filename(ordenes_file.filename) or 'ordenes_compra.xlsx'
        tasa_filename = secure_filename(tasa_file.filename) or 'tasa.xlsx'
        
        ruta_ordenes = os.path.join(temp_dir, ordenes_filename)
        ruta_tasa = os.path.join(temp_dir, tasa_filename)
        
        ordenes_file.save(ruta_ordenes)
        tasa_file.save(ruta_tasa)
        
        print(f"Archivos guardados temporalmente:")
        print(f"  Ordenes: {ruta_ordenes}")
        print(f"  Tasa: {ruta_tasa}")
        
        # Generar nombre de archivo de salida con fecha
        fecha_actual = datetime.now()
        fecha_formato = f"{fecha_actual.day}_{fecha_actual.month}_{fecha_actual.year}"
        nombre_archivo_salida = f"resultado_deuda_{fecha_formato}.xlsx"
        ruta_salida = os.path.join(temp_dir, nombre_archivo_salida)
        
        # Procesar archivos
        print("Iniciando procesamiento de archivos...")
        from src.venezuela import procesar_archivos, preparar_dataframe_bigquery, subir_a_bigquery, subir_excel_a_cloud_storage
        
        df_ordenes, df_tasa = procesar_archivos(ruta_ordenes, ruta_tasa, ruta_salida)
        
        print(f"✓ Procesamiento completado. Filas procesadas: {len(df_ordenes)}")
        
        # Verificar que el archivo se haya creado
        if not os.path.exists(ruta_salida):
            print("✗ Error: El archivo de salida no se creó")
            return jsonify({
                'status': 'error',
                'message': 'Error al generar el archivo de salida'
            }), 500
        
        # Preparar DataFrame para BigQuery
        df_bq = preparar_dataframe_bigquery(df_ordenes)
        
        # Subir a BigQuery
        project_id = os.getenv('GCP_PROJECT_ID')
        dataset_id = os.getenv('BQ_DATASET_ID', 'deuda_vzla')
        table_id = os.getenv('BQ_TABLE_ID', 'ordenes_compra')
        
        if project_id:
            print(f"Subiendo a BigQuery: {project_id}.{dataset_id}.{table_id}")
            subir_a_bigquery(df_bq, project_id, dataset_id, table_id)
        else:
            print("⚠ GCP_PROJECT_ID no configurado, omitiendo subida a BigQuery")
        
        # Subir Excel a Cloud Storage
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        url_cloud_storage = None
        
        if bucket_name:
            print(f"Subiendo Excel a Cloud Storage: {bucket_name}")
            url_cloud_storage = subir_excel_a_cloud_storage(ruta_salida, bucket_name, nombre_archivo_salida)
            if url_cloud_storage:
                print(f"✓ URL de Cloud Storage obtenida: {url_cloud_storage}")
            else:
                print("⚠ No se pudo obtener la URL de Cloud Storage")
        else:
            print("⚠ GCS_BUCKET_NAME no configurado, omitiendo subida a Cloud Storage")
        
        # Preparar respuesta JSON
        respuesta = {
            'status': 'success',
            'message': 'Archivo procesado exitosamente',
            'filas_procesadas': len(df_ordenes),
            'archivo': nombre_archivo_salida
        }
        
        # Siempre incluir la URL de Cloud Storage si está disponible
        if url_cloud_storage:
            respuesta['url_cloud_storage'] = url_cloud_storage
            respuesta['blob_url'] = url_cloud_storage  # Alias para mayor claridad
        else:
            respuesta['url_cloud_storage'] = None
            print("⚠ Advertencia: No se pudo obtener la URL de Cloud Storage para incluir en la respuesta")
        
        print(f"Respuesta JSON preparada: {respuesta}")
        return jsonify(respuesta), 200
        
    except Exception as e:
        print(f"✗ Error al procesar archivos: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f'Error al procesar los archivos: {str(e)}'
        }), 500
    
    finally:
        # Limpiar archivos temporales después de un delay
        # (En producción, podrías usar un task en background)
        try:
            import time
            time.sleep(1)  # Dar tiempo para que el archivo se envíe
            # Limpiar archivos temporales
            for file in os.listdir(temp_dir):
                try:
                    os.remove(os.path.join(temp_dir, file))
                except:
                    pass
            os.rmdir(temp_dir)
            print(f"✓ Archivos temporales eliminados")
        except Exception as e:
            print(f"⚠ Error al limpiar archivos temporales: {e}")


@app.route('/generar-deuda-bq', methods=['POST'])
def generar_deuda_bq():
    """
    Endpoint para generar el archivo de deuda procesado usando tasas desde BigQuery.
    Solo recibe el archivo de Ordenes de Compra; las tasas se leen de la tabla
    cxp_vzla.bcv_tasas en BigQuery.
    
    FormData:
        - ordenes_compra: Archivo Excel de Ordenes de Compra
    """
    print(f"Generar Deuda (BQ) - {request.method} {request.path}")
    
    # Verificar que se haya enviado el archivo
    if 'ordenes_compra' not in request.files:
        print("✗ Error: No se encontró el archivo 'ordenes_compra'")
        return jsonify({
            'status': 'error',
            'message': 'Falta el archivo de Ordenes de Compra. Use el campo "ordenes_compra"'
        }), 400
    
    ordenes_file = request.files['ordenes_compra']
    
    # Verificar que el archivo no esté vacío
    if ordenes_file.filename == '':
        print("✗ Error: El archivo de Ordenes de Compra está vacío")
        return jsonify({
            'status': 'error',
            'message': 'El archivo de Ordenes de Compra está vacío'
        }), 400
    
    # Verificar que GCP_PROJECT_ID esté configurado (necesario para leer tasas de BQ)
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        print("✗ Error: GCP_PROJECT_ID no está configurado")
        return jsonify({
            'status': 'error',
            'message': 'GCP_PROJECT_ID no está configurado. Se requiere para leer tasas desde BigQuery.'
        }), 500
    
    # Crear directorio temporal para los archivos
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Guardar archivo temporalmente
        ordenes_filename = secure_filename(ordenes_file.filename) or 'ordenes_compra.xlsx'
        ruta_ordenes = os.path.join(temp_dir, ordenes_filename)
        ordenes_file.save(ruta_ordenes)
        
        print(f"Archivo guardado temporalmente:")
        print(f"  Ordenes: {ruta_ordenes}")
        print(f"  Tasas: se leerán desde BigQuery (cxp_vzla.bcv_tasas)")
        
        # Generar nombre de archivo de salida con fecha
        fecha_actual = datetime.now()
        fecha_formato = f"{fecha_actual.day}_{fecha_actual.month}_{fecha_actual.year}"
        nombre_archivo_salida = f"resultado_deuda_{fecha_formato}.xlsx"
        ruta_salida = os.path.join(temp_dir, nombre_archivo_salida)
        
        # Procesar archivos usando tasas desde BigQuery
        print("Iniciando procesamiento de archivos (tasas desde BigQuery)...")
        from src.venezuela import procesar_archivos_con_bigquery, preparar_dataframe_bigquery, subir_a_bigquery, subir_excel_a_cloud_storage
        
        df_ordenes, df_tasa = procesar_archivos_con_bigquery(ruta_ordenes, ruta_salida, project_id)
        
        print(f"✓ Procesamiento completado. Filas procesadas: {len(df_ordenes)}")
        
        # Verificar que el archivo se haya creado
        if not os.path.exists(ruta_salida):
            print("✗ Error: El archivo de salida no se creó")
            return jsonify({
                'status': 'error',
                'message': 'Error al generar el archivo de salida'
            }), 500
        
        # Preparar DataFrame para BigQuery
        df_bq = preparar_dataframe_bigquery(df_ordenes)
        
        # Subir a BigQuery
        dataset_id = os.getenv('BQ_DATASET_ID', 'deuda_vzla')
        table_id = os.getenv('BQ_TABLE_ID', 'ordenes_compra')
        
        print(f"Subiendo a BigQuery: {project_id}.{dataset_id}.{table_id}")
        subir_a_bigquery(df_bq, project_id, dataset_id, table_id)
        
        # Subir Excel a Cloud Storage
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        url_cloud_storage = None
        
        if bucket_name:
            print(f"Subiendo Excel a Cloud Storage: {bucket_name}")
            url_cloud_storage = subir_excel_a_cloud_storage(ruta_salida, bucket_name, nombre_archivo_salida)
            if url_cloud_storage:
                print(f"✓ URL de Cloud Storage obtenida: {url_cloud_storage}")
            else:
                print("⚠ No se pudo obtener la URL de Cloud Storage")
        else:
            print("⚠ GCS_BUCKET_NAME no configurado, omitiendo subida a Cloud Storage")
        
        # Preparar respuesta JSON
        respuesta = {
            'status': 'success',
            'message': 'Archivo procesado exitosamente (tasas desde BigQuery)',
            'filas_procesadas': len(df_ordenes),
            'archivo': nombre_archivo_salida,
            'fuente_tasas': 'BigQuery (cxp_vzla.bcv_tasas)'
        }
        
        # Siempre incluir la URL de Cloud Storage si está disponible
        if url_cloud_storage:
            respuesta['url_cloud_storage'] = url_cloud_storage
            respuesta['blob_url'] = url_cloud_storage
        else:
            respuesta['url_cloud_storage'] = None
            print("⚠ Advertencia: No se pudo obtener la URL de Cloud Storage para incluir en la respuesta")
        
        print(f"Respuesta JSON preparada: {respuesta}")
        return jsonify(respuesta), 200
        
    except Exception as e:
        print(f"✗ Error al procesar archivos: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f'Error al procesar los archivos: {str(e)}'
        }), 500
    
    finally:
        # Limpiar archivos temporales
        try:
            import time
            time.sleep(1)
            for file in os.listdir(temp_dir):
                try:
                    os.remove(os.path.join(temp_dir, file))
                except:
                    pass
            os.rmdir(temp_dir)
            print(f"✓ Archivos temporales eliminados")
        except Exception as e:
            print(f"⚠ Error al limpiar archivos temporales: {e}")


# ============================================================
# Historial de archivos en Cloud Storage
# ============================================================

@app.route('/archivos', methods=['GET'])
def listar_archivos():
    """
    Lista todos los archivos procesados en el bucket de Cloud Storage (carpeta vzla/).
    Devuelve las URLs organizadas por fecha de ejecución (más reciente primero).
    
    Query params opcionales:
        - limit: Cantidad máxima de carpetas a devolver (default: 50)
    
    Response:
        {
            "status": "success",
            "total_ejecuciones": 5,
            "archivos": [
                {
                    "carpeta": "2026-02-12_15-30-45",
                    "fecha": "2026-02-12",
                    "hora": "15:30:45",
                    "archivos": [
                        {
                            "nombre": "resultado_deuda_12_2_2026.xlsx",
                            "url": "https://storage.googleapis.com/...",
                            "tamaño_bytes": 123456
                        }
                    ]
                }
            ]
        }
    """
    print(f"Listar Archivos - {request.method} {request.path}")
    
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    if not bucket_name:
        return jsonify({
            'status': 'error',
            'message': 'GCS_BUCKET_NAME no está configurado'
        }), 500
    
    limit = request.args.get('limit', 50, type=int)
    
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        
        # Listar todos los blobs bajo el prefijo vzla/
        blobs = bucket.list_blobs(prefix='vzla/')
        
        # Agrupar por carpeta de timestamp
        carpetas = {}
        for blob in blobs:
            # ruta: vzla/{timestamp}/{archivo}
            partes = blob.name.split('/')
            if len(partes) < 3 or not partes[1]:
                continue
            
            carpeta_ts = partes[1]
            nombre_archivo = '/'.join(partes[2:])
            
            # Ignorar "carpetas vacías" (objetos que terminan en /)
            if not nombre_archivo:
                continue
            
            if carpeta_ts not in carpetas:
                carpetas[carpeta_ts] = []
            
            # Construir URL pública
            url = f"https://storage.googleapis.com/{bucket_name}/{blob.name}"
            
            carpetas[carpeta_ts].append({
                'nombre': nombre_archivo,
                'url': url,
                'tamaño_bytes': blob.size,
                'ruta_completa': blob.name
            })
        
        # Ordenar por timestamp descendente (más reciente primero) y aplicar limit
        carpetas_ordenadas = sorted(carpetas.keys(), reverse=True)[:limit]
        
        archivos_respuesta = []
        for ts in carpetas_ordenadas:
            # Parsear fecha y hora del timestamp (formato: YYYY-MM-DD_HH-MM-SS)
            fecha = ''
            hora = ''
            try:
                partes_ts = ts.split('_')
                if len(partes_ts) >= 2:
                    fecha = partes_ts[0]
                    hora = partes_ts[1].replace('-', ':')
            except:
                pass
            
            archivos_respuesta.append({
                'carpeta': ts,
                'fecha': fecha,
                'hora': hora,
                'archivos': carpetas[ts]
            })
        
        print(f"✓ Archivos listados. Ejecuciones encontradas: {len(archivos_respuesta)}")
        
        return jsonify({
            'status': 'success',
            'total_ejecuciones': len(archivos_respuesta),
            'archivos': archivos_respuesta
        }), 200
        
    except Exception as e:
        print(f"✗ Error al listar archivos: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f'Error al listar archivos: {str(e)}'
        }), 500


# ============================================================
# SSE (Server-Sent Events) - Endpoints con progreso en tiempo real
# ============================================================

def sse_event(data: dict) -> str:
    """Formatea un evento SSE como string."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.route('/generar-deuda/stream', methods=['POST'])
def generar_deuda_stream():
    """
    Endpoint SSE para generar deuda con progreso en tiempo real.
    Tasas desde archivo Excel. Mismo comportamiento que /generar-deuda
    pero devuelve eventos SSE con el progreso de cada paso.
    
    FormData:
        - ordenes_compra: Archivo Excel de Ordenes de Compra
        - tasa: Archivo Excel de Tasa
    """
    print(f"Generar Deuda Stream - {request.method} {request.path}")
    
    # --- Validaciones sincronas (antes de iniciar el stream) ---
    if 'ordenes_compra' not in request.files:
        return jsonify({'status': 'error', 'message': 'Falta el archivo de Ordenes de Compra.'}), 400
    if 'tasa' not in request.files:
        return jsonify({'status': 'error', 'message': 'Falta el archivo de Tasa.'}), 400
    
    ordenes_file = request.files['ordenes_compra']
    tasa_file = request.files['tasa']
    
    if ordenes_file.filename == '':
        return jsonify({'status': 'error', 'message': 'El archivo de Ordenes de Compra está vacío'}), 400
    if tasa_file.filename == '':
        return jsonify({'status': 'error', 'message': 'El archivo de Tasa está vacío'}), 400
    
    # Guardar archivos en temp ANTES de iniciar el stream
    temp_dir = tempfile.mkdtemp()
    ordenes_filename = secure_filename(ordenes_file.filename) or 'ordenes_compra.xlsx'
    tasa_filename = secure_filename(tasa_file.filename) or 'tasa.xlsx'
    ruta_ordenes = os.path.join(temp_dir, ordenes_filename)
    ruta_tasa = os.path.join(temp_dir, tasa_filename)
    ordenes_file.save(ruta_ordenes)
    tasa_file.save(ruta_tasa)
    
    fecha_actual = datetime.now()
    fecha_formato = f"{fecha_actual.day}_{fecha_actual.month}_{fecha_actual.year}"
    nombre_archivo_salida = f"resultado_deuda_{fecha_formato}.xlsx"
    ruta_salida = os.path.join(temp_dir, nombre_archivo_salida)
    
    def generate():
        try:
            from src.venezuela import (
                leer_ordenes_compra, leer_tasa, filtrar_cerrados,
                agregar_ano_fiscal, agregar_columna_tasa, agregar_columna_area,
                agregar_montos_oc, agregar_montos_oc_asociado, agregar_monto_real_deuda,
                aplicar_estilos_excel, preparar_dataframe_bigquery, subir_a_bigquery,
                subir_excel_a_cloud_storage
            )
            import pandas as pd
            
            # Paso 1: Leer ordenes de compra
            yield sse_event({"type": "progress", "step": "Leyendo archivo de Órdenes de Compra...", "percent": 5})
            df_ordenes = leer_ordenes_compra(ruta_ordenes)
            yield sse_event({"type": "progress", "step": f"Órdenes leídas: {len(df_ordenes)} filas", "percent": 15})
            
            # Paso 2: Leer tasas desde archivo
            yield sse_event({"type": "progress", "step": "Leyendo archivo de Tasas...", "percent": 20})
            df_tasa = leer_tasa(ruta_tasa)
            yield sse_event({"type": "progress", "step": f"Tasas leídas: {len(df_tasa)} filas", "percent": 25})
            
            # Paso 3: Filtrar cerrados
            yield sse_event({"type": "progress", "step": "Filtrando órdenes cerradas...", "percent": 30})
            df_ordenes = filtrar_cerrados(df_ordenes)
            
            # Paso 4: Año fiscal
            yield sse_event({"type": "progress", "step": "Calculando Año Fiscal...", "percent": 35})
            df_ordenes = agregar_ano_fiscal(df_ordenes)
            
            # Paso 5: Tasa
            yield sse_event({"type": "progress", "step": "Asignando tasas de cambio a cada orden...", "percent": 40})
            df_ordenes = agregar_columna_tasa(df_ordenes, df_tasa)
            
            # Paso 6: Área
            yield sse_event({"type": "progress", "step": "Consultando áreas desde Google Sheets...", "percent": 50})
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
            df_ordenes = agregar_columna_area(
                df_ordenes, spreadsheet_id,
                credentials_path if credentials_path and os.path.exists(credentials_path) else None
            )
            
            # Paso 7: Montos OC
            yield sse_event({"type": "progress", "step": "Calculando Monto OC y Monto OC USD...", "percent": 60})
            df_ordenes = agregar_montos_oc(df_ordenes)
            
            # Paso 8: Montos OC Asociado
            yield sse_event({"type": "progress", "step": "Calculando Monto OC Asociado y Monto OC Asociado USD...", "percent": 65})
            df_ordenes = agregar_montos_oc_asociado(df_ordenes)
            
            # Paso 9: Monto Real Deuda
            yield sse_event({"type": "progress", "step": "Calculando Monto Real Deuda...", "percent": 70})
            df_ordenes = agregar_monto_real_deuda(df_ordenes)
            
            # Paso 10: Guardar Excel
            yield sse_event({"type": "progress", "step": "Generando archivo Excel...", "percent": 75})
            with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
                df_ordenes.to_excel(writer, sheet_name='Ordenes de Compra', index=False)
                df_tasa.to_excel(writer, sheet_name='Tasa', index=False)
            aplicar_estilos_excel(ruta_salida)
            
            # Paso 11: Subir a BigQuery
            project_id = os.getenv('GCP_PROJECT_ID')
            dataset_id = os.getenv('BQ_DATASET_ID', 'deuda_vzla')
            table_id = os.getenv('BQ_TABLE_ID', 'ordenes_compra')
            
            if project_id:
                yield sse_event({"type": "progress", "step": "Subiendo datos a BigQuery...", "percent": 85})
                df_bq = preparar_dataframe_bigquery(df_ordenes)
                subir_a_bigquery(df_bq, project_id, dataset_id, table_id)
            
            # Paso 12: Subir a Cloud Storage
            bucket_name = os.getenv('GCS_BUCKET_NAME')
            url_cloud_storage = None
            
            if bucket_name:
                yield sse_event({"type": "progress", "step": "Subiendo Excel a Cloud Storage...", "percent": 92})
                url_cloud_storage = subir_excel_a_cloud_storage(ruta_salida, bucket_name, nombre_archivo_salida)
            
            # Evento final
            yield sse_event({
                "type": "complete",
                "status": "success",
                "message": "Archivo procesado exitosamente",
                "filas_procesadas": len(df_ordenes),
                "archivo": nombre_archivo_salida,
                "url_cloud_storage": url_cloud_storage,
                "percent": 100
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield sse_event({"type": "error", "message": str(e)})
        
        finally:
            # Limpiar archivos temporales
            try:
                import time
                time.sleep(1)
                for f in os.listdir(temp_dir):
                    try:
                        os.remove(os.path.join(temp_dir, f))
                    except:
                        pass
                os.rmdir(temp_dir)
            except:
                pass
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


@app.route('/generar-deuda-bq/stream', methods=['POST'])
def generar_deuda_bq_stream():
    """
    Endpoint SSE para generar deuda con progreso en tiempo real.
    Tasas desde BigQuery (cxp_vzla.bcv_tasas). Mismo comportamiento
    que /generar-deuda-bq pero devuelve eventos SSE con el progreso.
    
    FormData:
        - ordenes_compra: Archivo Excel de Ordenes de Compra
    """
    print(f"Generar Deuda BQ Stream - {request.method} {request.path}")
    
    # --- Validaciones sincronas ---
    if 'ordenes_compra' not in request.files:
        return jsonify({'status': 'error', 'message': 'Falta el archivo de Ordenes de Compra.'}), 400
    
    ordenes_file = request.files['ordenes_compra']
    if ordenes_file.filename == '':
        return jsonify({'status': 'error', 'message': 'El archivo de Ordenes de Compra está vacío'}), 400
    
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        return jsonify({'status': 'error', 'message': 'GCP_PROJECT_ID no está configurado.'}), 500
    
    # Guardar archivo en temp ANTES de iniciar el stream
    temp_dir = tempfile.mkdtemp()
    ordenes_filename = secure_filename(ordenes_file.filename) or 'ordenes_compra.xlsx'
    ruta_ordenes = os.path.join(temp_dir, ordenes_filename)
    ordenes_file.save(ruta_ordenes)
    
    fecha_actual = datetime.now()
    fecha_formato = f"{fecha_actual.day}_{fecha_actual.month}_{fecha_actual.year}"
    nombre_archivo_salida = f"resultado_deuda_{fecha_formato}.xlsx"
    ruta_salida = os.path.join(temp_dir, nombre_archivo_salida)
    
    def generate():
        try:
            from src.venezuela import (
                leer_ordenes_compra, leer_tasa_desde_bigquery, filtrar_cerrados,
                agregar_ano_fiscal, agregar_columna_tasa, agregar_columna_area,
                agregar_montos_oc, agregar_montos_oc_asociado, agregar_monto_real_deuda,
                aplicar_estilos_excel, preparar_dataframe_bigquery, subir_a_bigquery,
                subir_excel_a_cloud_storage
            )
            import pandas as pd
            
            # Paso 1: Leer ordenes de compra
            yield sse_event({"type": "progress", "step": "Leyendo archivo de Órdenes de Compra...", "percent": 5})
            df_ordenes = leer_ordenes_compra(ruta_ordenes)
            yield sse_event({"type": "progress", "step": f"Órdenes leídas: {len(df_ordenes)} filas", "percent": 15})
            
            # Paso 2: Leer tasas desde BigQuery
            yield sse_event({"type": "progress", "step": "Consultando tasas BCV desde BigQuery...", "percent": 20})
            df_tasa = leer_tasa_desde_bigquery(project_id)
            yield sse_event({"type": "progress", "step": f"Tasas leídas: {len(df_tasa)} registros", "percent": 25})
            
            # Paso 3: Filtrar cerrados
            yield sse_event({"type": "progress", "step": "Filtrando órdenes cerradas...", "percent": 30})
            df_ordenes = filtrar_cerrados(df_ordenes)
            
            # Paso 4: Año fiscal
            yield sse_event({"type": "progress", "step": "Calculando Año Fiscal...", "percent": 35})
            df_ordenes = agregar_ano_fiscal(df_ordenes)
            
            # Paso 5: Tasa
            yield sse_event({"type": "progress", "step": "Asignando tasas de cambio a cada orden...", "percent": 40})
            df_ordenes = agregar_columna_tasa(df_ordenes, df_tasa)
            
            # Paso 6: Área
            yield sse_event({"type": "progress", "step": "Consultando áreas desde Google Sheets...", "percent": 50})
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
            df_ordenes = agregar_columna_area(
                df_ordenes, spreadsheet_id,
                credentials_path if credentials_path and os.path.exists(credentials_path) else None
            )
            
            # Paso 7: Montos OC
            yield sse_event({"type": "progress", "step": "Calculando Monto OC y Monto OC USD...", "percent": 60})
            df_ordenes = agregar_montos_oc(df_ordenes)
            
            # Paso 8: Montos OC Asociado
            yield sse_event({"type": "progress", "step": "Calculando Monto OC Asociado y Monto OC Asociado USD...", "percent": 65})
            df_ordenes = agregar_montos_oc_asociado(df_ordenes)
            
            # Paso 9: Monto Real Deuda
            yield sse_event({"type": "progress", "step": "Calculando Monto Real Deuda...", "percent": 70})
            df_ordenes = agregar_monto_real_deuda(df_ordenes)
            
            # Paso 10: Guardar Excel
            yield sse_event({"type": "progress", "step": "Generando archivo Excel...", "percent": 75})
            with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
                df_ordenes.to_excel(writer, sheet_name='Ordenes de Compra', index=False)
                df_tasa.to_excel(writer, sheet_name='Tasa', index=False)
            aplicar_estilos_excel(ruta_salida)
            
            # Paso 11: Subir a BigQuery
            dataset_id = os.getenv('BQ_DATASET_ID', 'deuda_vzla')
            table_id = os.getenv('BQ_TABLE_ID', 'ordenes_compra')
            
            yield sse_event({"type": "progress", "step": "Subiendo datos a BigQuery...", "percent": 85})
            df_bq = preparar_dataframe_bigquery(df_ordenes)
            subir_a_bigquery(df_bq, project_id, dataset_id, table_id)
            
            # Paso 12: Subir a Cloud Storage
            bucket_name = os.getenv('GCS_BUCKET_NAME')
            url_cloud_storage = None
            
            if bucket_name:
                yield sse_event({"type": "progress", "step": "Subiendo Excel a Cloud Storage...", "percent": 92})
                url_cloud_storage = subir_excel_a_cloud_storage(ruta_salida, bucket_name, nombre_archivo_salida)
            
            # Evento final
            yield sse_event({
                "type": "complete",
                "status": "success",
                "message": "Archivo procesado exitosamente (tasas desde BigQuery)",
                "filas_procesadas": len(df_ordenes),
                "archivo": nombre_archivo_salida,
                "fuente_tasas": "BigQuery (cxp_vzla.bcv_tasas)",
                "url_cloud_storage": url_cloud_storage,
                "percent": 100
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield sse_event({"type": "error", "message": str(e)})
        
        finally:
            # Limpiar archivos temporales
            try:
                import time
                time.sleep(1)
                for f in os.listdir(temp_dir):
                    try:
                        os.remove(os.path.join(temp_dir, f))
                    except:
                        pass
                os.rmdir(temp_dir)
            except:
                pass
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


# Middleware para imprimir cada request
@app.after_request
def log_request(response):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {request.method} {request.path} - {response.status_code}")
    return response

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    print(f"🚀 Iniciando servidor Flask en puerto {port}")
    print(f"📡 Endpoints disponibles:")
    print(f"   - GET  /health")
    print(f"   - GET  /test/bigquery")
    print(f"   - GET  /test/storage")
    print(f"   - GET  /test/sheets")
    print(f"   - POST /generar-deuda              (tasas desde archivo Excel)")
    print(f"   - POST /generar-deuda/stream        (idem, con progreso SSE)")
    print(f"   - POST /generar-deuda-bq            (tasas desde BigQuery)")
    print(f"   - POST /generar-deuda-bq/stream     (idem, con progreso SSE)")
    print(f"   - GET  /archivos                    (historial de archivos procesados)")
    app.run(host='0.0.0.0', port=port, debug=False)

