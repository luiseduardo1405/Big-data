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

print('\n── Top 10 clientes por RFM Score ──')
rfm_final.orderBy(desc('RFM_Score'))\
    .select('CustomerID', 'Recency', 'Frequency', 'Monetary',
            'R_score', 'F_score', 'M_score', 'RFM_Score', 'TipoCliente')\
    .show(10)

# ── Guardar ───────────────────────────────────────────────────
rfm_final.select('CustomerID', 'Recency', 'Frequency', 'Monetary',
                 'R_score', 'F_score', 'M_score', 'RFM_Score',
                 'prediction', 'TipoCliente')\
    .write.mode('overwrite')\
    .parquet('hdfs://tokito:8020/online_retail/output/predicciones/rfm_segmentado')

print('\n✅ Análisis RFM completado')
spark.stop()