# Detalle de la arquitectura/estructura del proyecto.

<img width="416" height="363" alt="image" src="https://github.com/user-attachments/assets/252b6e63-f677-47da-aaa5-5197d45c8b45" />

## DATA:

Dataset Legacy (Excel)
Emplazamientos_Centros_TX, Emplazamientos_Sitios_RAN, Emplazamientos_Infra (xlsx) 

## NOTEBOOKS:

- Capa Bronze (PySpark, Python, SQL)
Extraccion de data cruda de los dataset de los sistemas legacy.

- Capa Silver (SQL)
Limpieza, unificacion de criterios y tablas en una sola, que unifica los 3 modulos en 1 solo.

- Capa Gold (SQL)
Tablas listas para ser consumidas por BI, en cualquier herramienta de visualizacion (Tableau, Power BI, Qlik, Looker)

## DOCS:

- Arquitectura del proyecto
- Contexto de Negocio
- Diccionario de Terminos

