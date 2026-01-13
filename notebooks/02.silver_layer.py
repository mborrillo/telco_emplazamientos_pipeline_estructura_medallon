# ====================================================
# CAPA SILVER - Normalización y unificación
# ====================================================
# Este script normaliza nombres de columnas, tipos de datos
# y unifica los tres mundos de la capa Bronze en una vista única
# ====================================================

# Crear tabla SILVER: emplazamientos por módulo, conservando estado 
# y fecha de modificación de cada modulo

# Eliminar tabla si existe
spark.sql("DROP TABLE IF EXISTS silver_emplazamientos_modulo")

# Crear tabla silver_emplazamientos_modulo
spark.sql("""
CREATE OR REPLACE TABLE silver_emplazamientos_modulo
USING DELTA
AS

-- 1. Centros TX
SELECT
  'Centros TX'                         AS modulo,
  CAST(emplaz_TX AS STRING)            AS codigo_unico,
  UPPER(CAST(emplaz_TX AS STRING))     AS nombre_sitio,
  CAST(estado_centro AS STRING)        AS estado,
  CAST(creationDate AS TIMESTAMP)      AS fecha_modificacion,
  cabeceraOYM as cabeceraOYM,
  areaOYM as areaOYM

FROM raw_emplazamientos_centros_tx
WHERE
  TRIM(emplaz_TX)       <> ''      -- excluye los registros con código vacío
  AND estado_centro IS NOT NULL
  AND TRIM(estado_centro) <> ''   -- excluye los regustros con estado vacío

UNION ALL

-- 2. Sitios RAN
SELECT
  'Sitios RAN'                         AS modulo,
  CAST(emplaz_RAN AS STRING)           AS codigo_unico,
  UPPER(CAST(emplaz_RAN AS STRING))    AS nombre_sitio,
  CAST(estado_sitio AS STRING)         AS estado,
  CAST(lastChangeDate AS TIMESTAMP)    AS fecha_modificacion,
  cabecera as cabeceraOYM,
  region as areaOYM
FROM raw_emplazamientos_sitios_ran

UNION ALL

-- 3. Infraestructura
SELECT
  'Infraestructura'                    AS modulo,
  CAST(emplaz_INFRA AS STRING)         AS codigo_unico,
  UPPER(CAST(emplaz_INFRA AS STRING))  AS nombre_sitio,
  CAST(estado_infra AS STRING)         AS estado,
  CAST(creationDate AS TIMESTAMP)      AS fecha_modificacion,
  cabeceraOYM as cabeceraOYM,
  areaOYM as areaOYM
FROM raw_emplazamientos_infra
WHERE estado_infra NOT IN ('Baja de Necesidad','Reserva estrategica');
""")

# ====================================================
# Verificaciones de la capa Silver
# ====================================================

# Verificar conteo por módulo
print("Conteo de emplazamientos por módulo:")
spark.sql("""
SELECT modulo, COUNT(*) AS emplazamientos
FROM silver_emplazamientos_modulo
GROUP BY modulo;
""").show()

# Mostrar muestras por módulo
print("\nMuestra de Centros TX (5 registros):")
spark.sql("""
SELECT *
FROM silver_emplazamientos_modulo
WHERE modulo = 'Centros TX'
LIMIT 5;
""").show()

print("\nMuestra de Sitios RAN (5 registros):")
spark.sql("""
SELECT *
FROM silver_emplazamientos_modulo
WHERE modulo = 'Sitios RAN'
LIMIT 5;
""").show()

print("\nMuestra de Infraestructura (5 registros):")
spark.sql("""
SELECT *
FROM silver_emplazamientos_modulo
WHERE modulo = 'Infraestructura'
LIMIT 5;
""").show()