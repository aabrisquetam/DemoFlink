# PoC: Cálculo Dinámico de KPIs en Tiempo Real (Agbar)

**Migración de arquitectura Batch (Snowflake) a Streaming (Confluent Cloud Flink)**

Este repositorio contiene las instrucciones y sentencias SQL para desplegar la Prueba de Concepto (PoC) que demuestra cómo calcular fórmulas dinámicas de telemetría en tiempo real usando Apache Flink en Confluent Cloud.

## 🎯 Objetivo de la PoC
Demostrar que es posible sustituir el modelo de cálculo *batch* (donde Snowflake acumula datos, ordena arrays y calcula mediante vistas materializadas) por un modelo de **Streaming Continuo**. En esta arquitectura, Flink cruza los eventos de los sensores con un maestro de fórmulas en memoria y emite el KPI recalculado en cuestión de **milisegundos**.

---

## 🏗️ Arquitectura y Pasos de Despliegue

Abre un **Workspace de Flink** en tu entorno de Confluent Cloud y ejecuta los siguientes bloques de código de forma secuencial.

### Paso 1: Crear y Poblar el Maestro de KPIs
Creamos la tabla de búsqueda (*Lookup Table*) que relaciona cada sensor (Tag) con el KPI al que pertenece y la fórmula que le aplica.

```sql
-- 1. Crear la tabla Maestra (Confluent creará el tópico internamente)
CREATE TABLE kpi_maestro (
    id_kpi STRING,
    tag STRING,
    ds_kpi STRING,
    ds_unidad STRING,
    PRIMARY KEY (id_kpi, tag) NOT ENFORCED
);

-- 2. Insertar los mapeos base para la PoC (KPI_0008 y KPI_0010)
INSERT INTO kpi_maestro VALUES
    ('KPI_0008', 'SGABPRODUCCIO.AQMOS.SJD_001_FIX_001.FI001', 'Caudal de captación subterránea', 'm³/s'),
    ('KPI_0010', 'SGABPRODUCCIO.AQMOS.SJD_001_FIX_001.FI001', 'Caudal de captación total', 'm³/s'),
    ('KPI_0010', 'SGABPRODUCCIO.AQMOS.SJD_011_FCC_002.FI001', 'Caudal de captación total', 'm³/s'),
    ('KPI_0010', 'SGABPRODUCCIO.AQMOS.SJD_PE_FCC_0101.MFIV', 'Caudal de captación total', 'm³/s'),
    ('KPI_0010', 'SGABPRODUCCIO.AQMOS.SJD_001_SIS_001.FI052', 'Caudal de captación total', 'm³/s');
```

### Paso 2: Desplegar el Simulador de Telemetría (Data Generator)
Para la PoC, utilizamos el conector `faker` nativo de Confluent. Esto creará una tabla virtual que generará **1 evento por segundo** con valores aleatorios para nuestros 4 sensores críticos, simulando el flujo real de datos en movimiento.

```sql
CREATE TABLE telemetria_simulada (
    tag STRING,
    valor DOUBLE,
    event_time TIMESTAMP(3),
    -- El Watermark es obligatorio en streaming para gestionar la llegada del tiempo
    WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '1', 
    'fields.tag.expression' = '#{Options.option ''SGABPRODUCCIO.AQMOS.SJD_001_FIX_001.FI001'',''SGABPRODUCCIO.AQMOS.SJD_001_SIS_001.FI052'',''SGABPRODUCCIO.AQMOS.SJD_011_FCC_002.FI001'',''SGABPRODUCCIO.AQMOS.SJD_PE_FCC_0101.MFIV''}',
    'fields.valor.expression' = '#{Number.randomDouble ''2'',''0'',''100''}',
    'fields.event_time.expression' = '#{date.past ''5'',''SECONDS''}'
);
```

### Paso 3: Gestión de Estado (Deduplicación)
En streaming puro, no queremos procesar el máximo histórico de un sensor, sino su **última foto real**. Esta vista intermedia deduce el último valor conocido de cada Tag antes de enviarlo al motor de cálculo.

```sql
CREATE VIEW ultimo_estado_sensores AS
SELECT tag, valor, event_time
FROM (
    SELECT tag, valor, event_time,
           -- Ordenamos por tiempo y rescatamos el registro más reciente por Tag
           ROW_NUMBER() OVER (PARTITION BY tag ORDER BY event_time DESC) as rn
    FROM telemetria_simulada
) WHERE rn = 1;
```

### Paso 4: Crear la Tabla Destino (Soporte Upsert)
Esta es la tabla final de donde beberán los sistemas posteriores (ej. volcados a Redis o Dashboards). Incluye una `PRIMARY KEY`, lo que permite a Kafka sobrescribir (Upsert) el valor del KPI en lugar de añadir registros de forma infinita.

```sql
CREATE TABLE kpi_resultados_final (
    cd_kpi STRING,
    ds_kpi STRING,
    num_valor_kpi DOUBLE,
    ds_unidad STRING,
    fec_event_date TIMESTAMP(3),
    -- Clave primaria fundamental para actualizaciones en tiempo real (Upsert)
    PRIMARY KEY (cd_kpi) NOT ENFORCED
);
```

### Paso 5: El Motor de Cálculo Continuo (El Job Principal)
Este `INSERT INTO` es el proceso *core* que se queda en estado `RUNNING`. Cruza la telemetría deduplicada con el maestro y ejecuta las fórmulas matemáticas dinámicamente. 

*Nota técnica: La función `MAX()` combinada con el `CASE WHEN` se utiliza como técnica estándar de SQL (Pivot) para transformar filas en columnas durante el `GROUP BY`, rescatando el valor exacto de la fila correspondiente al sensor analizado.*

```sql
INSERT INTO kpi_resultados_final
SELECT 
    m.id_kpi AS cd_kpi,
    m.ds_kpi AS ds_kpi,
    
    -- Lógica de Fórmulas Dinámicas
    CASE 
        -- KPI_0008: Valor simple convertido
        WHEN m.id_kpi = 'KPI_0008' THEN 
            MAX(CASE WHEN t.tag = 'SGABPRODUCCIO.AQMOS.SJD_001_FIX_001.FI001' THEN t.valor ELSE 0.0 END) / 1000.0

        -- KPI_0010: Suma de múltiples sensores en tiempo real
        WHEN m.id_kpi = 'KPI_0010' THEN
            (MAX(CASE WHEN t.tag = 'SGABPRODUCCIO.AQMOS.SJD_001_FIX_001.FI001' THEN t.valor ELSE 0.0 END) / 1000.0)
            + 
            MAX(CASE WHEN t.tag = 'SGABPRODUCCIO.AQMOS.SJD_PE_FCC_0101.MFIV' THEN t.valor ELSE 0.0 END)
            
        ELSE 0.0 
    END AS num_valor_kpi,
    
    m.ds_unidad,
    MAX(t.event_time) AS fec_event_date
    
FROM ultimo_estado_sensores t
JOIN kpi_maestro m ON t.tag = m.tag
GROUP BY 
    m.id_kpi, 
    m.ds_kpi,
    m.ds_unidad;
```

---

## 🚀 Cómo verificar los resultados en la Demo

**⚠️ Importante:** Para la demostración con el cliente, **NO uses** `SELECT * FROM kpi_resultados_final` en el Workspace de Flink. La interfaz web realiza un *buffering* visual para no saturar el navegador y solo se actualiza cada 30-60 segundos. 

Para demostrar la velocidad y el rendimiento real del clúster (latencia < 50 ms):
1. Navega al menú lateral izquierdo de Confluent Cloud y selecciona **Topics**.
2. Busca y haz clic en el tópico `kpi_resultados_final`.
3. Selecciona la pestaña **Messages**.
4. Verás el flujo constante de mensajes JSON actualizándose segundo a segundo. Compara el campo `fec_event_date` (cuando se generó el dato simulado) con la hora de tu sistema para comprobar que la latencia de procesamiento es prácticamente nula.
