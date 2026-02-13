# Dockerfile para la aplicación de Deuda VZLA
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Limpiar cache de apt (no se necesitan dependencias adicionales)
RUN apt-get update && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copiar archivos de requisitos
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación
COPY src/ ./src/

# Crear directorio para datos temporales
RUN mkdir -p /app/data

# Exponer puerto
EXPOSE 8080

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()" || exit 1

# Comando para ejecutar la aplicación con gunicorn
# --access-logfile - y --error-logfile - para mostrar logs en stdout/stderr
# --log-level info para mostrar información de requests
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "300", "--access-logfile", "-", "--error-logfile", "-", "--log-level", "info", "src.api:app"]

