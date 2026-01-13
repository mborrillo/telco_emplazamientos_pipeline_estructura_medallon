# ====================================================
# CAPA BRONZE - Carga de datos crudos
# ====================================================
# Este script carga los datos crudos desde archivos Excel
# y crea las tablas temporales para los tres módulos:
# 1. Centros TX
# 2. Infraestructura
# 3. Sitios RAN
# ====================================================

import pandas as pd
import os

# Configuración de rutas - MODIFICAR según el entorno
volume_path = "/Volumes/workspace/default/iu_db_emplazamientos/bronze/"

# Listar archivos Excel en el directorio
files = [f for f in os.listdir(volume_path) if f.endswith('.xlsx')]

for file_name in files:
    full_path = os.path.join(volume_path, file_name)
    
    # 1. Leer el Excel sin convertir cadenas a NaN
    pdf = pd.read_excel(
        full_path,
        dtype=str,            # fuerza todo como texto
        keep_default_na=False # NO conviertas 'NA', 'null', '' en NaN
    )
    
    # 2. Reemplazar posibles None por cadena vacía para evitar "nan"
    pdf = pdf.fillna("")     # así no se convierte a "nan" en spark
    
    # 3. Crear DataFrame de Spark
    df = spark.createDataFrame(pdf)
    
    # 4. Crear tabla bronze/raw
    table_name = f"raw_{file_name.replace('.xlsx', '').replace(' ', '_').lower()}"
    df.createOrReplaceTempView(table_name)
    
    print(f"✅ Tabla '{table_name}' creada exitosamente")