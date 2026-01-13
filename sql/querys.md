## CAPA SILVER:
-- emplazamientos por módulo, conservando estado y fecha de modificación de cada modulo

DROP TABLE IF EXISTS silver_emplazamientos_modulo;

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
--WHERE estado_infra <> "Baja de Necesidad"
WHERE estado_infra NOT IN ('Baja de Necesidad','Reserva estrategica');

## CAPA GOLD:

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

%sql
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




