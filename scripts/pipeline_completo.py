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