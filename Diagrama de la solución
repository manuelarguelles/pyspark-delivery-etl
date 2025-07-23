## Descripción Gráfica de la Solución

```mermaid
flowchart TD
    %% Definición de clases para styling
    classDef inputClass fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#000
    classDef processClass fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef outputClass fill:#e8f5e8,stroke:#388e3c,stroke-width:2px,color:#000
    classDef decisionClass fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#000
    
    %% Subgrafo de Entradas
    subgraph INPUT [" 📥 ENTRADAS DEL PIPELINE "]
        direction TB
        A["📄 entregas_productos_prueba.csv<br/>🔹 <b>Datos Crudos de Entregas</b><br/>🔹 Formato: CSV"]
        B["⚙️ config.yaml<br/>🔹 <b>Reglas y Parámetros</b><br/>🔹 Configuración de Validación"]
    end
    
    %% Subgrafo de Procesamiento
    subgraph PROCESS [" ⚡ PROCESO ETL - PySpark/Databricks "]
        direction TB
        C["🔄 <b>1. Lectura y Carga de Datos</b><br/>📊 Spark DataFrame Loading"]
        D{"🔍 <b>2. Validación y Calidad</b><br/>📋 Data Quality Checks<br/>✅ Reglas de Negocio"}
        E["🔧 <b>3. Transformaciones de Negocio</b><br/>📅 Filtrado por fecha/país<br/>📏 Normalización de unidades<br/>📦 Clasificación de entregas<br/>🔤 Estandarización de columnas"]
    end
    
    %% Subgrafo de Salidas
    subgraph OUTPUT [" 📤 SALIDAS GENERADAS "]
        direction TB
        F["🗄️ <b>Datos Procesados (Silver)</b><br/>📁 Formato: Parquet<br/>🗂️ Particionado por fecha_proceso<br/>✅ Listos para Análisis"]
        G["⚠️ <b>Registros en Cuarentena</b><br/>📁 Formato: Parquet<br/>📝 Contiene motivo del rechazo<br/>🔍 Para Revisión Manual"]
    end
    
    %% Conexiones del Flujo Principal
    A --> C
    B --> C
    C --> D
    D -->|"✅ Registros Válidos"| E
    D -->|"❌ Registros Inválidos"| G
    E --> F
    
    %% Aplicar clases de styling
    class A,B inputClass
    class C,E processClass
    class D decisionClass
    class F,G outputClass
    
    %% Styling de subgrafos
    style INPUT fill:#f8f9fa,stroke:#343a40,stroke-width:3px
    style PROCESS fill:#f8f9fa,stroke:#343a40,stroke-width:3px
    style OUTPUT fill:#f8f9fa,stroke:#343a40,stroke-width:3px
```
