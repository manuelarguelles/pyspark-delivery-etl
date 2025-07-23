# Pipeline ETL de Procesamiento de Entregas

Este proyecto contiene un pipeline ETL completo desarrollado en PySpark para la prueba técnica. La solución procesa datos de entregas de productos, aplicando una serie de transformaciones, validaciones de calidad y reglas de negocio, todo controlado de manera centralizada a través de un archivo de configuración.

## Entorno de Desarrollo y Dependencias

La solución fue desarrollada y probada exitosamente en el siguiente entorno de Databricks:

- **Databricks Runtime:** 17.0
- **Apache Spark:** 4.0.0
- **Python:** 3.11 (o la versión que corresponda a tu DBR)
- **Librerías Principales:** `pyspark`, `omegaconf`, `PyYAML`

### Estructura del Repositorio
├── config/
│ └── config.yaml # Archivo de configuración base (opcional, ya que el notebook lo genera)
├── data/
│ └── entregas_productos_prueba.csv # Datos de entrada
├── notebooks/
│ └── Prueba_Tecnica_Final.py # El notebook principal exportado como script .py
├── README.md # Este archivo
└── requirements.txt # Dependencias del proyecto


## Instalación

1.  Clona este repositorio en tu máquina local.
2.  Para configurar un entorno local de prueba, crea un entorno virtual e instala las dependencias:
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

## Cómo Ejecutar en Databricks

1.  **Subir los Datos:** Sube el archivo `data/entregas_productos_prueba.csv` al Databricks File System (DBFS) en la siguiente ruta:
    ```
    dbfs:/FileStore/etl_pipeline_final/bronze/
    ```

2.  **Importar el Notebook:** Importa el archivo `notebooks/Prueba_Tecnica_Final.py` a tu Workspace de Databricks. La plataforma lo convertirá automáticamente en un notebook con celdas.

3.  **Ejecutar el Notebook:** Abre el notebook importado, asegúrate de que esté adjunto a un clúster con un Runtime compatible (DBR 17.0 o similar) y ejecuta todas las celdas en orden.

## Descripción de la Solución

El notebook está estructurado en cuatro fases principales que demuestran el ciclo de vida completo del procesamiento de datos:

1.  **Configuración y Herramientas:** Se prepara el entorno, se genera dinámicamente el archivo de configuración `config.yaml` y se cargan las librerías.
2.  **Definición del Pipeline:** Se encapsula toda la lógica ETL en una clase `DataPipeline` para un código limpio y mantenible.
3.  **Ejecución del Pipeline:** Se orquesta la ejecución para múltiples escenarios (diferentes países y fechas) para probar la flexibilidad de la solución.
4.  **Auditoría de Resultados:** Se verifica que los datos procesados y los datos en cuarentena se hayan escrito correctamente en las carpetas de salida.
