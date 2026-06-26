# Scripts del Proyecto Online Retail II — Comandos Separados

> Cada script se crea con `nano` en la ruta indicada, se pega el contenido, se guarda (`Ctrl+O`, `Enter`, `Ctrl+X`), y se ejecuta con el comando `python3` correspondiente.

---

## 01_ingesta.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/01_ingesta.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('OnlineRetail-Ingesta')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.csv(
    'hdfs://tokito:8020/online_retail/raw/online_retail.csv',
    header=True,
    inferSchema=True
)

print(f'\n Total filas:    {df.count():,}')
print(f' Total columnas: {len(df.columns)}')
print(f'\n Columnas: {df.columns}')
print(f'\n Schema:')
df.printSchema()
print(f'\n Muestra:')
df.show(5, truncate=False)

spark.stop()
print('\n✅ Ingesta verificada')
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/01_ingesta.py
```

---

## 02_etl.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/02_etl.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, dayofweek, round

spark = (
    SparkSession.builder
    .appName('OnlineRetail-ETL')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.csv(
    'hdfs://tokito:8020/online_retail/raw/online_retail.csv',
    header=True,
    inferSchema=True
)

filas_brutas = df.count()
print(f'\n Filas brutas: {filas_brutas:,}')

# 1 — Eliminar facturas canceladas (Invoice empieza con C)
df = df.filter(~col('Invoice').startswith('C'))
print(f' Tras eliminar canceladas: {df.count():,}')

# 2 — Eliminar Customer ID nulos
df = df.filter(col('Customer ID').isNotNull())
print(f' Tras eliminar sin cliente: {df.count():,}')

# 3 — Eliminar cantidades y precios negativos o cero
df = df.filter((col('Quantity') > 0) & (col('Price') > 0))
print(f' Tras eliminar negativos: {df.count():,}')

# 4 — Renombrar Customer ID (quitar el espacio)
df = df.withColumnRenamed('Customer ID', 'CustomerID')

# 5 — Crear columna TotalAmount
df = df.withColumn('TotalAmount', round(col('Quantity') * col('Price'), 2))

# 6 — Extraer features de fecha
df = df.withColumn('Year',      year(col('InvoiceDate'))) \
       .withColumn('Month',     month(col('InvoiceDate'))) \
       .withColumn('Day',       dayofmonth(col('InvoiceDate'))) \
       .withColumn('Hour',      hour(col('InvoiceDate'))) \
       .withColumn('DayOfWeek', dayofweek(col('InvoiceDate')))

filas_limpias = df.count()
print(f'\n Filas limpias:  {filas_limpias:,}')
print(f' Filas eliminadas: {filas_brutas - filas_limpias:,} ({((filas_brutas-filas_limpias)/filas_brutas*100):.1f}%)')
print(f'\n Muestra limpia:')
df.show(3, truncate=False)

# 7 — Guardar en HDFS como Parquet
df.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/processed/data_limpia')
print('\n✅ ETL completado — datos guardados en /online_retail/processed/')

spark.stop()
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/02_etl.py
```

---

## 03_kpis.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/03_kpis.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, sum, count, round, desc, min, max, stddev, when, log

spark = (
    SparkSession.builder
    .appName('OnlineRetail-KPIs')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet('hdfs://tokito:8020/online_retail/processed/data_limpia')

# ── KPI 1: Ticket Promedio por factura ──────────────────────
tickets_por_factura = df.groupBy('Invoice').agg(
    sum('TotalAmount').alias('TicketTotal')
)
ticket = tickets_por_factura.agg(
    round(avg('TicketTotal'), 2).alias('Ticket_Promedio'),
    round(min('TicketTotal'), 2).alias('Ticket_Minimo'),
    round(max('TicketTotal'), 2).alias('Ticket_Maximo'),
    round(stddev('TicketTotal'), 2).alias('Desviacion_Ticket'),
    round(stddev('TicketTotal') / avg('TicketTotal'), 4).alias('CV_Ticket')
)
print('\n── KPI 1: Ticket Promedio ──')
ticket.show()

# ── KPI 2: Top 10 productos más vendidos ────────────────────
ingreso_global = df.agg(sum('TotalAmount')).collect()[0][0]
top_productos = df.groupBy('StockCode', 'Description').agg(
    sum('Quantity').alias('UnidadesVendidas'),
    round(sum('TotalAmount'), 2).alias('IngresoTotal'),
    round(avg('Quantity'), 1).alias('PromUnidadesPedido'),
    round(sum('Quantity') / countDistinct('InvoiceDate'), 2).alias('VelocidadRotacion'),
    round(sum('TotalAmount') / sum('Quantity'), 2).alias('Ingreso_por_Unidad')
).withColumn(
    'PctIngreso', round(col('IngresoTotal') * 100.0 / ingreso_global, 4)
).orderBy(desc('UnidadesVendidas')).limit(10)
print('── KPI 2: Top 10 Productos por Rotación ──')
top_productos.show(truncate=False)

# ── KPI 3: Ingresos por País ─────────────────────────────────
por_pais = df.groupBy('Country').agg(
    round(sum('TotalAmount'), 2).alias('IngresoTotal'),
    countDistinct('CustomerID').alias('ClientesUnicos'),
    countDistinct('Invoice').alias('Facturas')
).withColumn(
    'PctMercado', round(col('IngresoTotal') * 100.0 / ingreso_global, 2)
).withColumn(
    'LTV_Pais', round(col('IngresoTotal') / col('ClientesUnicos'), 2)
).withColumn(
    'Facturas_por_Cliente', round(col('Facturas') / col('ClientesUnicos'), 2)
).orderBy(desc('IngresoTotal'))
print('── KPI 3: Ingresos por País ──')
por_pais.show(10)

# ── KPI 4: Flujo de Caja Mensual ─────────────────────────────
df.createOrReplaceTempView('ventas')
flujo_mensual = spark.sql("""
    WITH monthly AS (
        SELECT Year, Month,
               ROUND(SUM(TotalAmount), 2)   AS IngresosMes,
               COUNT(DISTINCT Invoice)       AS Facturas,
               COUNT(DISTINCT CustomerID)    AS ClientesActivos
        FROM ventas GROUP BY Year, Month
    )
    SELECT Year, Month, IngresosMes, Facturas, ClientesActivos,
           ROUND(
               (IngresosMes - LAG(IngresosMes) OVER (ORDER BY Year, Month))
               / LAG(IngresosMes) OVER (ORDER BY Year, Month) * 100, 2
           ) AS MoM_Growth_Pct,
           ROUND(IngresosMes * 100.0 / AVG(IngresosMes) OVER (), 2) AS Indice_Estacionalidad
    FROM monthly ORDER BY Year, Month
""")
print('── KPI 4: Flujo de Caja Mensual ──')
flujo_mensual.show(24)

# ── KPI 5: Segmentación B2B vs B2C ───────────────────────────
segmentos = spark.sql("""
    WITH clientes AS (
        SELECT CustomerID,
               SUM(TotalAmount)        AS gasto_total,
               COUNT(DISTINCT Invoice)  AS num_facturas
        FROM ventas GROUP BY CustomerID
    ),
    totales AS (SELECT SUM(gasto_total) AS ingreso_global FROM clientes)
    SELECT CASE WHEN c.gasto_total > 5000 THEN 'B2B' ELSE 'B2C' END AS Segmento,
           COUNT(*)                                    AS NumClientes,
           ROUND(AVG(c.gasto_total), 2)               AS GastoPromedio,
           ROUND(AVG(c.num_facturas), 1)              AS FacturasPromedio,
           ROUND(SUM(c.gasto_total), 2)               AS IngresoSegmento,
           ROUND(SUM(c.gasto_total) * 100.0 / MAX(t.ingreso_global), 2) AS PctIngreso,
           ROUND(SUM(c.gasto_total) / COUNT(*), 2)    AS VPC
    FROM clientes c CROSS JOIN totales t GROUP BY Segmento
""")
print('── KPI 5: B2B vs B2C ──')
segmentos.show()

# ── KPI 6: Riesgo de Quiebre de Stock ────────────────────────
alertas = spark.sql("""
    SELECT StockCode, Description,
           ROUND(STDDEV(Quantity), 2)                              AS Desviacion,
           ROUND(AVG(Quantity), 2)                                 AS PromedioUnidades,
           COUNT(Invoice)                                          AS NumPedidos,
           ROUND(STDDEV(Quantity) / AVG(Quantity), 2)             AS CoefVariacion,
           ROUND(STDDEV(Quantity) / AVG(Quantity) * LOG(COUNT(Invoice)), 2) AS RiskScore,
           CASE
               WHEN STDDEV(Quantity) / AVG(Quantity) > 8  THEN 'Critico'
               WHEN STDDEV(Quantity) / AVG(Quantity) >= 5 THEN 'Alto'
               WHEN STDDEV(Quantity) / AVG(Quantity) >= 2 THEN 'Medio'
               ELSE 'Bajo'
           END AS NivelRiesgo
    FROM ventas GROUP BY StockCode, Description
    HAVING COUNT(Invoice) > 20 AND STDDEV(Quantity) IS NOT NULL
    ORDER BY RiskScore DESC LIMIT 10
""")
print('── KPI 6: Riesgo Quiebre Stock ──')
alertas.show(truncate=False)

# Guardar KPIs
top_productos.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/top_productos')
por_pais.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/por_pais')
flujo_mensual.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/flujo_mensual')
alertas.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/inventario/alertas_stock')

print('\n✅ KPIs completados y guardados')
spark.stop()
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/03_kpis.py
```

---

## 04_inventario.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/04_inventario.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round, desc, countDistinct, stddev, when, log

spark = (
    SparkSession.builder
    .appName('OnlineRetail-Inventario')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet('hdfs://tokito:8020/online_retail/processed/data_limpia')

# ── ANÁLISIS 1: Velocidad de rotación por producto ───────────
print('\n── Velocidad de Rotación (unidades/día activo) ──')
rotacion = df.groupBy('StockCode', 'Description').agg(
    sum('Quantity').alias('TotalUnidades'),
    round(sum('TotalAmount'), 2).alias('IngresoTotal'),
    countDistinct('InvoiceDate').alias('DiasConVentas'),
    round(avg('Quantity'), 2).alias('PromUnidadesPorPedido')
).withColumn(
    'UnidadesPorDia', round(col('TotalUnidades') / col('DiasConVentas'), 2)
).orderBy(desc('UnidadesPorDia')).limit(10)
rotacion.show(truncate=False)

# ── ANÁLISIS 2: Productos con ventas muy irregulares (riesgo quiebre stock) ──
print('── Productos con Alta Variabilidad (Riesgo Quiebre Stock) ──')
variabilidad = df.groupBy('StockCode', 'Description').agg(
    round(stddev('Quantity'), 2).alias('Desviacion'),
    round(avg('Quantity'), 2).alias('PromedioUnidades'),
    count('Invoice').alias('NumPedidos')
).filter(
    (col('NumPedidos') > 20) & (col('Desviacion').isNotNull())
).withColumn(
    'CoefVariacion', round(col('Desviacion') / col('PromedioUnidades'), 2)
).withColumn(
    'RiskScore', round(col('CoefVariacion') * log(col('NumPedidos')), 2)
).withColumn(
    'NivelRiesgo',
    when(col('CoefVariacion') > 8,  'Critico')
    .when(col('CoefVariacion') >= 5, 'Alto')
    .when(col('CoefVariacion') >= 2, 'Medio')
    .otherwise('Bajo')
).orderBy(desc('RiskScore')).limit(10)
variabilidad.show(truncate=False)

# ── ANÁLISIS 3: Comportamiento B2B vs B2C ────────────────────
print('── Segmentación B2B vs B2C por tamaño de pedido ──')
segmentacion = df.groupBy('CustomerID', 'Country').agg(
    round(sum('TotalAmount'), 2).alias('GastoTotal'),
    round(avg('TotalAmount'), 2).alias('TicketPromedio'),
    countDistinct('Invoice').alias('NumFacturas'),
    sum('Quantity').alias('TotalUnidades')
)

b2b = segmentacion.filter(col('GastoTotal') > 5000).count()
b2c = segmentacion.filter(col('GastoTotal') <= 5000).count()
print(f'\n Clientes B2B (gasto > £5000): {b2b:,}')
print(f' Clientes B2C (gasto ≤ £5000): {b2c:,}')

print('\n Top 10 clientes B2B:')
segmentacion.filter(col('GastoTotal') > 5000)\
    .orderBy(desc('GastoTotal')).show(10)

# ── ANÁLISIS 4: Alertas de stock bajo ────────────────────────
print('── Alertas: Productos que dejaron de venderse en 2011 ──')
vendidos_2010 = df.filter(col('Year') == 2010)\
    .select('StockCode').distinct()
vendidos_2011 = df.filter(col('Year') == 2011)\
    .select('StockCode').distinct()

desaparecidos = vendidos_2010.subtract(vendidos_2011)
print(f'\n Productos vendidos en 2010 pero NO en 2011: {desaparecidos.count():,}')
print(' (Posibles quiebres de stock o descontinuados)')

# Guardar
rotacion.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/inventario/rotacion')
variabilidad.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/inventario/variabilidad')

print('\n✅ Análisis de inventario completado')
spark.stop()
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/04_inventario.py
```

---

## 05_ml.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/05_ml.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, round, datediff, lit, desc, count, avg, when, max as spark_max, ntile
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

spark = (
    SparkSession.builder
    .appName('OnlineRetail-RFM')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet('hdfs://tokito:8020/online_retail/processed/data_limpia')

# ── RFM ──────────────────────────────────────────────────────
fecha_referencia = df.agg(spark_max('InvoiceDate')).collect()[0][0]
print(f'\n Fecha de referencia: {fecha_referencia}')

rfm = df.groupBy('CustomerID').agg(
    datediff(lit(fecha_referencia), spark_max('InvoiceDate')).alias('Recency'),
    countDistinct('Invoice').alias('Frequency'),
    round(sum('TotalAmount'), 2).alias('Monetary')
)

# ── KMeans ───────────────────────────────────────────────────
assembler = VectorAssembler(
    inputCols=['Recency', 'Frequency', 'Monetary'],
    outputCol='features_raw'
)
scaler = StandardScaler(
    inputCol='features_raw',
    outputCol='features',
    withStd=True,
    withMean=True
)
kmeans = KMeans(featuresCol='features', k=4, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
modelo = pipeline.fit(rfm)
rfm_segmentado = modelo.transform(rfm)

# ── RFM Score (ntile 1-5 por dimensión) ──────────────────────
win_r = Window.orderBy(col('Recency').desc())    # menor Recency → score 5
win_f = Window.orderBy(col('Frequency').asc())   # mayor Frequency → score 5
win_m = Window.orderBy(col('Monetary').asc())    # mayor Monetary → score 5

rfm_segmentado = rfm_segmentado \
    .withColumn('R_score', ntile(5).over(win_r)) \
    .withColumn('F_score', ntile(5).over(win_f)) \
    .withColumn('M_score', ntile(5).over(win_m)) \
    .withColumn('RFM_Score', col('R_score') * 100 + col('F_score') * 10 + col('M_score'))

# ── Perfil por segmento ───────────────────────────────────────
print('\n── Perfil de cada segmento ──')
perfil = rfm_segmentado.groupBy('prediction').agg(
    count('CustomerID').alias('NumClientes'),
    round(avg('Recency'), 0).alias('Recency_Prom'),
    round(avg('Frequency'), 1).alias('Frequency_Prom'),
    round(avg('Monetary'), 2).alias('Monetary_Prom'),
    round(avg('RFM_Score'), 1).alias('RFM_Score_Prom')
).orderBy('Monetary_Prom', ascending=False)
perfil.show()

# ── Etiquetas ─────────────────────────────────────────────────
rfm_final = rfm_segmentado.withColumn(
    'TipoCliente',
    when(col('Monetary') > 10000, 'VIP - Alto Valor')
    .when(col('Recency') < 30,    'Activo Reciente')
    .when(col('Recency') < 90,    'En Riesgo')
    .otherwise('Perdido')
)

print('\n── Distribución tipos de cliente ──')
rfm_final.groupBy('TipoCliente').count().orderBy(desc('count')).show()

print('\n── Top 10 clientes VIP ──')
rfm_final.filter(col('TipoCliente') == 'VIP - Alto Valor')\
    .orderBy(desc('Monetary'))\
    .select('CustomerID', 'Recency', 'Frequency', 'Monetary', 'TipoCliente')\
    .show(10)

# ── Guardar ───────────────────────────────────────────────────
print('\n── Top 10 clientes por RFM Score ──')
rfm_final.orderBy(desc('RFM_Score'))\
    .select('CustomerID', 'Recency', 'Frequency', 'Monetary',
            'R_score', 'F_score', 'M_score', 'RFM_Score', 'TipoCliente')\
    .show(10)

rfm_final.select('CustomerID', 'Recency', 'Frequency', 'Monetary',
                 'R_score', 'F_score', 'M_score', 'RFM_Score',
                 'prediction', 'TipoCliente')\
    .write.mode('overwrite')\
    .parquet('hdfs://tokito:8020/online_retail/output/predicciones/rfm_segmentado')

print('\n✅ Análisis RFM completado')
spark.stop()
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/05_ml.py
```

---

## pipeline_completo.py

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/scripts/pipeline_completo.py
```

**Contenido:**
```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sum, count, avg, round, desc, countDistinct,
    datediff, lit, max as spark_max, when, stddev, year, month, dayofmonth, hour, dayofweek,
    ntile, log)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

spark = (
    SparkSession.builder
    .appName('OnlineRetail-Pipeline-Completo')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.instances', '2')
    .config('spark.executor.cores', '1')
    .config('spark.sql.shuffle.partitions', '8')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

print('=' * 60)
print(' PIPELINE COMPLETO — ONLINE RETAIL II')
print('=' * 60)

# ══════════════════════════════════════════════════════════════
# FASE 1 — INGESTA
# ══════════════════════════════════════════════════════════════
print('\n[1/6] INGESTA...')
df_raw = spark.read.csv(
    'hdfs://tokito:8020/online_retail/raw/online_retail.csv',
    header=True, inferSchema=True
)
print(f'     Filas cargadas: {df_raw.count():,}')

# ══════════════════════════════════════════════════════════════
# FASE 2 — ETL
# ══════════════════════════════════════════════════════════════
print('\n[2/6] ETL — Limpieza y transformación...')
df = df_raw.filter(~col('Invoice').startswith('C')) \
           .filter(col('Customer ID').isNotNull()) \
           .filter((col('Quantity') > 0) & (col('Price') > 0)) \
           .withColumnRenamed('Customer ID', 'CustomerID') \
           .withColumn('TotalAmount', round(col('Quantity') * col('Price'), 2)) \
           .withColumn('Year',      year(col('InvoiceDate'))) \
           .withColumn('Month',     month(col('InvoiceDate'))) \
           .withColumn('Day',       dayofmonth(col('InvoiceDate'))) \
           .withColumn('Hour',      hour(col('InvoiceDate'))) \
           .withColumn('DayOfWeek', dayofweek(col('InvoiceDate')))

df.cache()
filas_limpias = df.count()
print(f'     Filas limpias:   {filas_limpias:,}')
print(f'     Eliminadas:      {df_raw.count() - filas_limpias:,} (24.5%)')
df.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/processed/data_limpia')
print('     ✓ Datos limpios guardados en HDFS')

# ══════════════════════════════════════════════════════════════
# FASE 3 — SPARK SQL (KPIs)
# ══════════════════════════════════════════════════════════════
print('\n[3/6] SPARK SQL — KPIs estratégicos...')
df.createOrReplaceTempView('ventas')

ticket = spark.sql("""
    SELECT ROUND(AVG(ticket_total), 2)                         AS Ticket_Promedio,
           ROUND(MIN(ticket_total), 2)                         AS Ticket_Minimo,
           ROUND(MAX(ticket_total), 2)                         AS Ticket_Maximo,
           ROUND(STDDEV(ticket_total), 2)                      AS Desviacion_Ticket,
           ROUND(STDDEV(ticket_total) / AVG(ticket_total), 4)  AS CV_Ticket
    FROM (SELECT Invoice, SUM(TotalAmount) AS ticket_total FROM ventas GROUP BY Invoice)
""")
print('\n── KPI 1: Ticket Promedio ──')
ticket.show()

top_productos = spark.sql("""
    SELECT StockCode, Description,
           SUM(Quantity)                                            AS UnidadesVendidas,
           ROUND(SUM(TotalAmount), 2)                              AS IngresoTotal,
           ROUND(AVG(Quantity), 1)                                 AS PromUnidadesPedido,
           ROUND(SUM(Quantity) / COUNT(DISTINCT InvoiceDate), 2)   AS VelocidadRotacion,
           ROUND(SUM(TotalAmount) / SUM(Quantity), 2)              AS Ingreso_por_Unidad,
           ROUND(SUM(TotalAmount) * 100.0 / SUM(SUM(TotalAmount)) OVER (), 4) AS PctIngreso
    FROM ventas
    GROUP BY StockCode, Description
    ORDER BY UnidadesVendidas DESC
    LIMIT 10
""")
print('── KPI 2: Top 10 Productos ──')
top_productos.show(truncate=False)

por_pais = spark.sql("""
    SELECT Country,
           ROUND(SUM(TotalAmount), 2)                               AS IngresoTotal,
           COUNT(DISTINCT CustomerID)                               AS ClientesUnicos,
           COUNT(DISTINCT Invoice)                                  AS Facturas,
           ROUND(AVG(TotalAmount), 2)                              AS TicketPromedio,
           ROUND(SUM(TotalAmount) * 100.0 / SUM(SUM(TotalAmount)) OVER (), 2) AS PctMercado,
           ROUND(SUM(TotalAmount) / COUNT(DISTINCT CustomerID), 2) AS LTV_Pais,
           ROUND(COUNT(DISTINCT Invoice) * 1.0 / COUNT(DISTINCT CustomerID), 2) AS Facturas_por_Cliente
    FROM ventas
    GROUP BY Country
    ORDER BY IngresoTotal DESC
    LIMIT 10
""")
print('── KPI 3: Ingresos por País ──')
por_pais.show()

flujo_mensual = spark.sql("""
    WITH monthly AS (
        SELECT Year, Month,
               ROUND(SUM(TotalAmount), 2)   AS IngresosMes,
               COUNT(DISTINCT Invoice)       AS Facturas,
               COUNT(DISTINCT CustomerID)    AS ClientesActivos
        FROM ventas
        GROUP BY Year, Month
    )
    SELECT Year, Month, IngresosMes, Facturas, ClientesActivos,
           ROUND(
               (IngresosMes - LAG(IngresosMes) OVER (ORDER BY Year, Month))
               / LAG(IngresosMes) OVER (ORDER BY Year, Month) * 100, 2
           ) AS MoM_Growth_Pct,
           ROUND(IngresosMes * 100.0 / AVG(IngresosMes) OVER (), 2) AS Indice_Estacionalidad
    FROM monthly
    ORDER BY Year, Month
""")
print('── KPI 4: Flujo de Caja Mensual ──')
flujo_mensual.show(24)

segmentos = spark.sql("""
    WITH clientes AS (
        SELECT CustomerID,
               SUM(TotalAmount)        AS gasto_total,
               COUNT(DISTINCT Invoice)  AS num_facturas
        FROM ventas GROUP BY CustomerID
    ),
    totales AS (SELECT SUM(gasto_total) AS ingreso_global FROM clientes)
    SELECT CASE WHEN c.gasto_total > 5000 THEN 'B2B' ELSE 'B2C' END AS Segmento,
           COUNT(*)                                    AS NumClientes,
           ROUND(AVG(c.gasto_total), 2)               AS GastoPromedio,
           ROUND(AVG(c.num_facturas), 1)              AS FacturasPromedio,
           ROUND(SUM(c.gasto_total), 2)               AS IngresoSegmento,
           ROUND(SUM(c.gasto_total) * 100.0 / MAX(t.ingreso_global), 2) AS PctIngreso,
           ROUND(SUM(c.gasto_total) / COUNT(*), 2)    AS VPC
    FROM clientes c CROSS JOIN totales t
    GROUP BY Segmento
""")
print('── KPI 5: B2B vs B2C ──')
segmentos.show()

alertas = spark.sql("""
    SELECT StockCode, Description,
           ROUND(STDDEV(Quantity), 2)                              AS Desviacion,
           ROUND(AVG(Quantity), 2)                                 AS PromedioUnidades,
           COUNT(Invoice)                                          AS NumPedidos,
           ROUND(STDDEV(Quantity) / AVG(Quantity), 2)             AS CoefVariacion,
           ROUND(STDDEV(Quantity) / AVG(Quantity) * LOG(COUNT(Invoice)), 2) AS RiskScore,
           CASE
               WHEN STDDEV(Quantity) / AVG(Quantity) > 8  THEN 'Critico'
               WHEN STDDEV(Quantity) / AVG(Quantity) >= 5 THEN 'Alto'
               WHEN STDDEV(Quantity) / AVG(Quantity) >= 2 THEN 'Medio'
               ELSE 'Bajo'
           END AS NivelRiesgo
    FROM ventas GROUP BY StockCode, Description
    HAVING COUNT(Invoice) > 20 AND STDDEV(Quantity) IS NOT NULL
    ORDER BY RiskScore DESC LIMIT 10
""")
print('── KPI 6: Riesgo Quiebre Stock ──')
alertas.show(truncate=False)

top_productos.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/top_productos')
por_pais.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/por_pais')
flujo_mensual.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/kpis/flujo_mensual')
print('     ✓ KPIs guardados en HDFS')

# ══════════════════════════════════════════════════════════════
# FASE 4 — MLlib (RFM + KMeans)
# ══════════════════════════════════════════════════════════════
print('\n[4/6] MLlib — Segmentación RFM con KMeans...')
fecha_ref = df.agg(spark_max('InvoiceDate')).collect()[0][0]

rfm = df.groupBy('CustomerID').agg(
    datediff(lit(fecha_ref), spark_max('InvoiceDate')).alias('Recency'),
    countDistinct('Invoice').alias('Frequency'),
    round(sum('TotalAmount'), 2).alias('Monetary')
)

assembler = VectorAssembler(inputCols=['Recency', 'Frequency', 'Monetary'], outputCol='features_raw')
scaler    = StandardScaler(inputCol='features_raw', outputCol='features', withStd=True, withMean=True)
kmeans    = KMeans(featuresCol='features', k=4, seed=42)

modelo         = Pipeline(stages=[assembler, scaler, kmeans]).fit(rfm)
rfm_segmentado = modelo.transform(rfm)

# ── RFM Score (ntile 1-5 por dimensión) ──────────────────────
win_r = Window.orderBy(col('Recency').desc())    # menor Recency → score 5
win_f = Window.orderBy(col('Frequency').asc())   # mayor Frequency → score 5
win_m = Window.orderBy(col('Monetary').asc())    # mayor Monetary → score 5

rfm_segmentado = rfm_segmentado \
    .withColumn('R_score', ntile(5).over(win_r)) \
    .withColumn('F_score', ntile(5).over(win_f)) \
    .withColumn('M_score', ntile(5).over(win_m)) \
    .withColumn('RFM_Score', col('R_score') * 100 + col('F_score') * 10 + col('M_score'))

rfm_final = rfm_segmentado.withColumn(
    'TipoCliente',
    when(col('Monetary') > 10000, 'VIP - Alto Valor')
    .when(col('Recency') < 30,    'Activo Reciente')
    .when(col('Recency') < 90,    'En Riesgo')
    .otherwise('Perdido')
)

print('\n── Perfil de Segmentos ──')
rfm_segmentado.groupBy('prediction').agg(
    count('CustomerID').alias('Clientes'),
    round(avg('Recency'), 0).alias('Recency_Prom'),
    round(avg('Frequency'), 1).alias('Frequency_Prom'),
    round(avg('Monetary'), 2).alias('Monetary_Prom'),
    round(avg('RFM_Score'), 1).alias('RFM_Score_Prom')
).orderBy(desc('Monetary_Prom')).show()

print('\n── Top 10 clientes por RFM Score ──')
rfm_segmentado.select('CustomerID', 'Recency', 'Frequency', 'Monetary',
                      'R_score', 'F_score', 'M_score', 'RFM_Score') \
    .orderBy(desc('RFM_Score')).show(10)

print('── Distribución Tipos de Cliente ──')
rfm_final.groupBy('TipoCliente').count().orderBy(desc('count')).show()

# ══════════════════════════════════════════════════════════════
# FASE 5 — ESCRITURA HDFS
# ══════════════════════════════════════════════════════════════
print('\n[5/6] ESCRITURA — Guardando en HDFS...')
rfm_final.select('CustomerID', 'Recency', 'Frequency', 'Monetary',
                 'R_score', 'F_score', 'M_score', 'RFM_Score',
                 'prediction', 'TipoCliente') \
    .write.mode('overwrite') \
    .parquet('hdfs://tokito:8020/online_retail/output/predicciones/rfm_segmentado')
alertas.write.mode('overwrite').parquet('hdfs://tokito:8020/online_retail/output/inventario/alertas_stock')
print('     ✓ Resultados guardados en HDFS')

# ══════════════════════════════════════════════════════════════
# FASE 6 — EXPORTAR CSVs PARA DASHBOARD
# ══════════════════════════════════════════════════════════════
print('\n[6/6] DASHBOARD — Exportando CSVs para Streamlit...')
import os as _os
_os.makedirs('/home/hadoop/online_retail/dashboard/data', exist_ok=True)

exportaciones = {
    'top_productos': 'hdfs://tokito:8020/online_retail/output/kpis/top_productos',
    'por_pais':      'hdfs://tokito:8020/online_retail/output/kpis/por_pais',
    'flujo_mensual': 'hdfs://tokito:8020/online_retail/output/kpis/flujo_mensual',
    'rfm':           'hdfs://tokito:8020/online_retail/output/predicciones/rfm_segmentado',
    'alertas_stock': 'hdfs://tokito:8020/online_retail/output/inventario/alertas_stock',
}
for nombre, ruta in exportaciones.items():
    spark.read.parquet(ruta).toPandas()\
        .to_csv(f'/home/hadoop/online_retail/dashboard/data/{nombre}.csv', index=False)
    print(f'     ✓ {nombre}.csv exportado')

print('\n' + '=' * 60)
print(' ✅ PIPELINE COMPLETO FINALIZADO')
print(' 📊 Dashboard: streamlit run /home/hadoop/online_retail/dashboard/app.py')
print('=' * 60)

df.unpersist()
spark.stop()
```

**Ejecutar:**
```bash
python3 /home/hadoop/online_retail/scripts/pipeline_completo.py
```

---

## Dashboard (app.py)

**Crear el archivo:**
```bash
nano /home/hadoop/online_retail/dashboard/app.py
```

**Contenido:**
```python
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title='Online Retail II — Dashboard',
    page_icon='🛒',
    layout='wide'
)

@st.cache_data
def cargar_datos():
    base = '/home/hadoop/online_retail/dashboard/data/'
    return {
        'top_productos': pd.read_csv(base + 'top_productos.csv'),
        'por_pais':      pd.read_csv(base + 'por_pais.csv'),
        'flujo_mensual': pd.read_csv(base + 'flujo_mensual.csv'),
        'rfm':           pd.read_csv(base + 'rfm.csv'),
        'alertas':       pd.read_csv(base + 'alertas_stock.csv'),
    }

data = cargar_datos()

st.title('🛒 Online Retail II — Análisis Estratégico Comercial')
st.markdown('**Dataset:** UCI Online Retail II | **Periodo:** 2009–2011 | **Registros limpios:** 805,549')
st.divider()

col1, col2, col3, col4 = st.columns(4)
col1.metric('Ticket Promedio',   '£479.95',  '↑ vs industria')
col2.metric('Clientes B2B',      '670',      '11.4% del total')
col3.metric('Clientes Perdidos', '2,960',    '50.4%')
col4.metric('Países activos',    '37',       'mercados')
st.divider()

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader('📈 Flujo de Caja Mensual')
    flujo = data['flujo_mensual'].copy()
    flujo['Periodo'] = flujo['Year'].astype(str) + '-' + flujo['Month'].astype(str).str.zfill(2)
    fig = px.area(flujo, x='Periodo', y='IngresosMes',
                  color_discrete_sequence=['#00B4D8'],
                  labels={'IngresosMes': 'Ingresos (£)', 'Periodo': 'Mes'})
    fig.update_layout(height=300, margin=dict(t=20))
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader('🏢 B2B vs B2C')
    fig2 = go.Figure(go.Pie(
        labels=['B2C (5,208)', 'B2B (670)'],
        values=[5208, 670],
        hole=0.5,
        marker_colors=['#0077B6', '#00B4D8']
    ))
    fig2.update_layout(height=300, margin=dict(t=20))
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader('📦 Top 10 Productos por Rotación')
    top = data['top_productos'].head(10)
    fig3 = px.bar(top, x='UnidadesVendidas', y='Description',
                  orientation='h', color='IngresoTotal',
                  color_continuous_scale='Blues',
                  labels={'Description': '', 'UnidadesVendidas': 'Unidades'})
    fig3.update_layout(height=350, margin=dict(t=20), yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig3, use_container_width=True)

with col_right2:
    st.subheader('🌍 Ingresos por País (Top 10)')
    pais = data['por_pais'].head(10)
    fig4 = px.bar(pais, x='Country', y='IngresoTotal',
                  color='IngresoTotal', color_continuous_scale='Blues',
                  labels={'IngresoTotal': 'Ingresos (£)', 'Country': ''})
    fig4.update_layout(height=350, margin=dict(t=20))
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

st.subheader('👥 Segmentación RFM de Clientes')
col_rfm1, col_rfm2 = st.columns(2)

with col_rfm1:
    rfm_dist = data['rfm']['TipoCliente'].value_counts().reset_index()
    rfm_dist.columns = ['TipoCliente', 'count']
    colores = {
        'VIP - Alto Valor': '#023E8A',
        'Activo Reciente':  '#0096C7',
        'En Riesgo':        '#90E0EF',
        'Perdido':          '#CAF0F8'
    }
    fig5 = px.pie(rfm_dist, values='count', names='TipoCliente',
                  color='TipoCliente', color_discrete_map=colores)
    fig5.update_layout(height=300)
    st.plotly_chart(fig5, use_container_width=True)

with col_rfm2:
    st.subheader('⚠️ Alertas de Riesgo de Quiebre de Stock')
    alertas = data['alertas'][['Description', 'CoefVariacion', 'NumPedidos']].head(10)
    fig6 = px.bar(alertas, x='CoefVariacion', y='Description',
                  orientation='h', color='CoefVariacion',
                  color_continuous_scale='Reds',
                  labels={'CoefVariacion': 'Coef. Variación', 'Description': ''})
    fig6.update_layout(height=300, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig6, use_container_width=True)

st.divider()

st.subheader('🔍 Detalle Clientes VIP')
vip = data['rfm'][data['rfm']['TipoCliente'] == 'VIP - Alto Valor']\
    .sort_values('Monetary', ascending=False)\
    [['CustomerID', 'Recency', 'Frequency', 'Monetary', 'TipoCliente']]\
    .head(15)
st.dataframe(vip, use_container_width=True)
```

**Instalar dependencias (una sola vez):**
```bash
pip install streamlit plotly --break-system-packages
```

**Ejecutar:**
```bash
streamlit run /home/hadoop/online_retail/dashboard/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0
```

**Acceder desde Windows (túnel SSH):**
```powershell
ssh -L 8501:localhost:8501 hadoop@100.126.227.85
```

Luego abrir en el navegador: **http://localhost:8501**
