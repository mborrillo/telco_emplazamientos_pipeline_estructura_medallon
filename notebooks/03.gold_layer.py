# ====================================================
# CAPA GOLD - Transformaciones analíticas
# ====================================================
# Este script crea tablas analíticas para responder a preguntas
# de negocio/operación a partir de los datos normalizados
# ====================================================

# ====================================================
# 1. Evaluación de estados por módulo
# ====================================================
print("Listado de estados por módulo:")
spark.sql("""
SELECT
  modulo,
  estado,
  COUNT(*) AS num_filas
FROM silver_emplazamientos_modulo
GROUP BY modulo, estado
ORDER BY modulo, estado;
""").show()

# ====================================================
# 2. Backlog de pendientes por módulo
# ====================================================
# Tabla: ¿Qué centros están pendientes en cada módulo?
spark.sql("""
DROP TABLE IF EXISTS gold_centros_pendientes_por_modulo;

CREATE OR REPLACE TABLE gold_centros_pendientes_por_modulo
USING DELTA
AS
SELECT
  modulo,
  codigo_unico,
  nombre_sitio,
  estado,
  fecha_modificacion,
  cabeceraOYM,
  areaOYM
FROM silver_emplazamientos_modulo
WHERE UPPER(estado) IN ('PLANIFICADO', 'LIBERADO', 'DESDEFINIDO', 'BAJA', 'ACTIVO', 'NO UTILIZADO', 'RESTITUIDO', 'A RESTITUIR');
""")

print("Muestra de gold_centros_pendientes_por_modulo:")
spark.sql("SELECT * FROM gold_centros_pendientes_por_modulo LIMIT 5").show()

# ====================================================
# 3. Resumen de backlog por región / cabecera
# ====================================================
# Tabla: ¿Cuántos centros pendientes tiene cada región?
spark.sql("""
DROP TABLE IF EXISTS gold_backlog_resumen_oym;

CREATE OR REPLACE TABLE gold_backlog_resumen_oym
USING DELTA
AS
SELECT
  modulo,
  cabeceraOYM,
  areaOYM,
  COUNT(*) AS num_centros_pendientes
FROM gold_centros_pendientes_por_modulo
GROUP BY modulo, cabeceraOYM, areaOYM;
""")

print("Muestra de gold_backlog_resumen_oym:")
spark.sql("SELECT * FROM gold_backlog_resumen_oym LIMIT 5").show()

# ====================================================
# 4. Estado consolidado por emplazamiento (cross-módulo)
# ====================================================
# Tabla: ¿Qué estado tiene cada emplazamiento en cada módulo?
spark.sql("""
DROP TABLE IF EXISTS gold_emplazamientos_estado_cross_modulo;

CREATE OR REPLACE TABLE gold_emplazamientos_estado_cross_modulo
USING DELTA
AS
SELECT
  codigo_unico,

  MAX(CASE WHEN modulo = 'Centros TX'      THEN nombre_sitio END) AS nombre_sitio_ref,

  MAX(CASE WHEN modulo = 'Centros TX'      THEN estado END) AS estado_tx,
  MAX(CASE WHEN modulo = 'Sitios RAN'      THEN estado END) AS estado_ran,
  MAX(CASE WHEN modulo = 'Infraestructura' THEN estado END) AS estado_infra,

  MAX(CASE WHEN modulo = 'Centros TX'      THEN fecha_modificacion END) AS fecha_tx,
  MAX(CASE WHEN modulo = 'Sitios RAN'      THEN fecha_modificacion END) AS fecha_ran,
  MAX(CASE WHEN modulo = 'Infraestructura' THEN fecha_modificacion END) AS fecha_infra
FROM silver_emplazamientos_modulo
GROUP BY codigo_unico;
""")

print("Muestra de gold_emplazamientos_estado_cross_modulo:")
spark.sql("SELECT * FROM gold_emplazamientos_estado_cross_modulo LIMIT 5").show()

# ====================================================
# 5. Emplazamientos con inconsistencias de estado entre módulos
# ====================================================
# Tabla: ¿Qué centros tienen estados distintos entre módulos?
spark.sql("""
DROP TABLE IF EXISTS gold_emplazamientos_inconsistencias_estado;

CREATE OR REPLACE TABLE gold_emplazamientos_inconsistencias_estado
USING DELTA
AS
WITH base AS (
  SELECT *
  FROM gold_emplazamientos_estado_cross_modulo
),
norm AS (
  SELECT
    codigo_unico,
    UPPER(COALESCE(estado_tx, 'SIN_DATO'))   AS estado_tx,
    UPPER(COALESCE(estado_ran, 'SIN_DATO'))  AS estado_ran,
    UPPER(COALESCE(estado_infra, 'SIN_DATO')) AS estado_infra
  FROM base
)
SELECT *
FROM norm
WHERE NOT (estado_tx = estado_ran AND estado_ran = estado_infra);
""")

print("Muestra de gold_emplazamientos_inconsistencias_estado:")
spark.sql("SELECT * FROM gold_emplazamientos_inconsistencias_estado LIMIT 5").show()

# ====================================================
# 6. Emplazamientos inactivos / obsoletos por antigüedad
# ====================================================
# Tabla: ¿Qué emplazamientos no se han tocado desde hace X días?
spark.sql("""
DROP TABLE IF EXISTS gold_emplazamientos_sin_actualizacion;

CREATE OR REPLACE TABLE gold_emplazamientos_sin_actualizacion
USING DELTA
AS
SELECT
  modulo,
  codigo_unico,
  nombre_sitio,
  estado,
  fecha_modificacion,
  cabeceraOYM,
  areaOYM,
  datediff(current_date(), CAST(fecha_modificacion AS DATE)) AS dias_desde_ultima_modificacion
FROM silver_emplazamientos_modulo
WHERE fecha_modificacion IS NOT NULL
  AND datediff(current_date(), CAST(fecha_modificacion AS DATE)) > 180;
""")

print("Muestra de gold_emplazamientos_sin_actualizacion:")
spark.sql("SELECT * FROM gold_emplazamientos_sin_actualizacion LIMIT 5").show()

# ====================================================
# 7. Evolución Mensual de Altas/Bajas por Módulo
# ====================================================
print("Evolución mensual de emplazamientos por módulo (últimos registros):")
spark.sql("""
SELECT
  modulo,
  DATE_FORMAT(fecha_modificacion, 'yyyy-MM') AS yearMonth,
  estado,
  areaOYM,
  cabeceraOYM,
  COUNT(*) AS num_emplazamientos
FROM silver_emplazamientos_modulo
WHERE fecha_modificacion IS NOT NULL
GROUP BY modulo, DATE_FORMAT(fecha_modificacion, 'yyyy-MM'), estado, areaOYM, cabeceraOYM
ORDER BY yearMonth DESC, modulo, estado
LIMIT 100;
""").show(truncate=False)

print("✅ Proceso de capa GOLD completado exitosamente")
print("Tablas creadas:")
print("1. gold_centros_pendientes_por_modulo")
print("2. gold_backlog_resumen_oym")
print("3. gold_emplazamientos_estado_cross_modulo")
print("4. gold_emplazamientos_inconsistencias_estado")
print("5. gold_emplazamientos_sin_actualizacion")