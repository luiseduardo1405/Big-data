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