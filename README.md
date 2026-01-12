# Emplazamientos de Red Telco – Databricks Pipeline Medallón (Bronze–Silver–Gold)

Este proyecto implementa un pipeline de datos en Databricks para unificar tres sistemas de emplazamientos de red telco: **Centros TX**, **Sitios RAN** e **Infraestructura**.
Históricamente cada módulo operaba por separado; aquí se construye un modelo único de emplazamientos que permite analizar estados, antigüedad de actualizaciones y disponibilidad por región y área operativa.

## Arquitectura de datos

El pipeline sigue un enfoque clásico de capas:

- **Fuentes**: ficheros Excel provenientes de los tres módulos (TX, RAN, Infraestructura).  
- **Capa Bronze**: carga cruda de los Excels en tablas `raw_emplazamientos_*`, preservando al máximo la información original. 
- **Capa Silver**: normalización de nombres y tipos de columnas y unificación de los tres mundos en una única vista de emplazamientos con esquema homogéneo.[file:21]  
- **Capa Gold**: construcción de tablas analíticas:
  - Tabla maestra por `codigo_unico` con estados y fechas por módulo.
  - Tablas agregadas mensuales por módulo, cabecera y `areaOYM`.
  - Métricas de disponibilidad de emplazamientos por área (totales, disponibles y porcentaje disponible).

En `docs/architecture.md` se describe el diagrama completo desde los Excels hasta las tablas Gold y la futura conexión con Power BI.

## Dataset

Los datos representan emplazamientos de una compañía telco ficticia, exportados desde tres sistemas distintos (TX, RAN, Infraestructura) y consolidados en Databricks.

En este repositorio se incluyen solo ficheros de ejemplo en la carpeta `data/`:

- `sample_centros_tx.xlsx`
- `sample_infra.xlsx`
- `sample_sitios_ran.xlsx`

Estos ficheros son extractos y/o datos simulados que permiten reproducir la lógica del pipeline sin exponer información sensible real.

## Pipeline paso a paso

El pipeline se implementa en tres notebooks principales:

- `notebooks/01_bronze_ingestion.py`  
  - Lee automáticamente todos los ficheros Excel de un volumen de Databricks.  
  - Fuerza los campos a tipo texto para evitar problemas de tipado.  
  - Evita la conversión automática a `NaN` y crea tablas `raw_emplazamientos_centros_tx`, `raw_emplazamientos_infra` y `raw_emplazamientos_sitios_ran`.

- `notebooks/02_silver_unification.py`  
  - Selecciona y renombra columnas comunes (código único, nombre de sitio, estado, fecha, cabecera, área, etc.).  
  - Añade una columna `modulo` para identificar el origen (TX, RAN, Infraestructura).  
  - Unifica los tres orígenes en una vista única de emplazamientos, permitiendo análisis consistentes entre módulos.

- `notebooks/03_gold_analytics.py`  
  - Construye una tabla maestra por `codigo_unico` combinando la información de los tres módulos.  
  - Calcula métricas temporales como `dias_desde_ultima_modificacion`.  
  - Genera tablas agregadas mensuales por módulo, cabecera y `areaOYM`, y calcula ratios de disponibilidad de emplazamientos por área.

En la carpeta `sql/` se incluyen los scripts SQL que definen las vistas Silver y las tablas/vistas Gold utilizadas por herramientas de BI.

## Casos de uso de negocio

El modelo unificado de emplazamientos permite responder preguntas y obtener hallazgaos:
¿Qué centros están pendientes en cada módulo, con su última fecha y localización OYM?
¿Cuántos centros pendientes tiene cada región / cabecera por módulo?
¿Qué centros tienen estados distintos entre módulos (por ejemplo, cerrado en uno y pendiente en otro)
Evolución Mensual de Altas/Bajas por Módulo
Evolución Mensual de Emplazamientos No Productivos por Módulo

Este pipeline sirve como base para un tablero de Power BI que consuma directamente las tablas Gold.

## Cómo reproducir el proyecto

1. Clonar este repositorio.  
2. Subir los ficheros de `data/` a un Volume o ruta accesible desde tu cluster de Databricks.  
3. Ajustar en `01_bronze_ingestion.py` la ruta base del volumen (`volume_path`) según tu entorno. 
4. Ejecutar los notebooks en orden: 01 → 02 → 03.  
5. (Opcional) Crear vistas o tablas físicas a partir de las tablas Gold y conectarlas desde Power BI u otra herramienta de BI.

## Próximos pasos

- Añadir un reporte en Power BI conectado a las tablas Gold, con páginas dedicadas a:
  - Disponibilidad por `areaOYM`.
  - Evolución temporal de estados por módulo.
  - Fichas de emplazamiento con detalle TX/RAN/Infraestructura.
- Incorporar reglas de negocio adicionales para clasificar estados y construir indicadores de salud de red más avanzados.

