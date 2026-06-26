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