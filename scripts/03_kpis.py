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