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