# workshop_02

Este taller es un ejercicio práctico sobre cómo construir un pipeline ETL utilizando Apache Airflow y otras tecnologías. El objetivo es aprender a extraer información de diversas fuentes de datos, realizar transformaciones, cargar datos en una base de datos y crear un panel de visualización.

## Requisitos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes:

- Apache Airflow: Un entorno de orquestación de tareas.
- Python: Lenguaje de programación utilizado para escribir los scripts ETL.

## Descripción

En este taller, construiremos un pipeline ETL que realiza las siguientes tareas:

1. **Extracción de Datos**:
   - Fuentes de datos:
     - Archivo CSV (Spotify Tracks Dataset).
     - Base de datos (grammy_awards en mysql).

2. **Transformación de Datos**:
   - Realizaremos varias transformaciones en los datos según las necesidades del proyecto, estas estan especificadas en etl_02workshop.py

3. **Carga de Datos**:
   - Almacenaremos los datos en Google Drive como archivo CSV.
   - Guardaremos los datos transformados en una base de datos (proporcionar detalles).

4. **Creación del dashboard**:
   - Usaremos una herramienta de visualización  Power BI para visualizar los datos.

## Ejecución del Taller

Para ejecutar este taller, sigue estos pasos:

1. [Clonar este repositorio](#) en tu máquina local.
2. Asegúrate de que cumples con los requisitos mencionados anteriormente.
3. Configura Apache Airflow según sea necesario (archivos DAG, conexiones, etc.).
4. Ejecuta los DAGs en Apache Airflow.

## Autor

- [Kevin Artunduaga]
