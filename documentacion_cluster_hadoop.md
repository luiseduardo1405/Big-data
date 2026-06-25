# Documentación Completa: Clúster Hadoop Distribuido con Worker en la Nube

## Arquitectura Final

| Nodo | Hostname | IP Tailscale | Rol |
|------|----------|-------------|-----|
| Master | tokito | 100.126.227.85 | NameNode · SecondaryNameNode · ResourceManager · Spark Driver |
| Worker 1 | worker1 | 100.107.94.42 | DataNode · NodeManager · Spark Executor |
| Worker 2 | worker2 | 100.112.192.60 | DataNode · NodeManager · Spark Executor (DigitalOcean) |

### Daemons por nodo

**Master (tokito)**
- `NameNode` — gestiona el sistema de archivos HDFS
- `SecondaryNameNode` — checkpoints periódicos del NameNode
- `ResourceManager` — planificador de recursos YARN
- `Spark Driver` — coordina los jobs de Spark

**Workers (worker1, worker2)**
- `DataNode` — almacena bloques de datos HDFS
- `NodeManager` — ejecuta contenedores de tareas YARN
- `Spark Executor` — procesa tareas distribuidas de Spark

### Stack tecnológico

| Componente | Versión |
|------------|---------|
| Hadoop | 3.3.6 |
| Java | OpenJDK 1.8.0_412 (Temurin) |
| PySpark | 3.5.0 |
| Python | 3.13.5 |
| OS worker2 | Debian 13 (Trixie) |
| VPN | Tailscale |

---

## Parte 1 — Creación del Droplet (worker2) en DigitalOcean

### Especificaciones del Droplet

| Parámetro | Valor |
|-----------|-------|
| Imagen | Debian 13 x64 |
| Plan | Basic — Regular CPU (SSD) |
| vCPU | 4 |
| RAM | 8 GB |
| Disco | 160 GB SSD |
| Transferencia | 5 TB |
| Costo | $48.00/mes |
| Nombre | hadoop-worker2 |
| Región | NYC1 (Nueva York) |
| Autenticación | SSH Key (clave pública de Windows) |

> **Nota:** Para futuros Droplets, usar la región **São Paulo (BRA1)** para menor latencia desde Perú.

### Generar SSH Key en Windows (antes de crear el Droplet)

```powershell
# En PowerShell de Windows
ssh-keygen -t rsa -b 4096
# Presionar Enter en todo (sin passphrase)

# Ver la clave pública para agregarla a DigitalOcean
type $env:USERPROFILE\.ssh\id_rsa.pub
```

Agregar la clave pública en DigitalOcean: **Settings → Security → SSH Keys → Add SSH Key**

### Conectarse al Droplet

```powershell
ssh root@IP_PUBLICA_DROPLET
```

### Instalar el agente de DigitalOcean (para acceso al web console)

```bash
curl -sSL https://repos-droplet.digitalocean.com/install.sh | sudo bash
systemctl status droplet-agent
```

---

## Parte 2 — Configuración de Red con Tailscale

Tailscale crea una VPN mesh que permite que tokito (red local) y worker2 (DigitalOcean) se comuniquen como si estuvieran en la misma red.

### Instalación en tokito y worker2

```bash
# Ejecutar en AMBAS máquinas
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
# Autenticarse con la misma cuenta en ambas máquinas
```

### Verificar conectividad

```bash
# Ver la IP asignada por Tailscale
tailscale ip -4

# Verificar que se ven entre sí
ping -c 2 100.107.94.42   # worker1 desde tokito
ping -c 2 100.112.192.60  # worker2 desde tokito
ping -c 2 100.126.227.85  # tokito desde worker2
```

---

## Parte 3 — Configuración inicial de worker2

### Crear usuario hadoop

```bash
# En worker2 como root
adduser hadoop
usermod -aG sudo hadoop
```

### Configurar SSH para el usuario hadoop

```bash
su - hadoop
ssh-keygen -t rsa -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh
exit
```

### Crear directorio de instalación

```bash
mkdir -p /opt/hadoop
chown hadoop:hadoop /opt/hadoop
```

---

## Parte 4 — Configurar /etc/hosts en todos los nodos

Agregar las IPs de Tailscale en **tokito, worker1 y worker2**:

**En tokito:**
```bash
sudo nano /etc/hosts
```
```
127.0.0.1           localhost
100.126.227.85      tokito
100.107.94.42       worker1
100.112.192.60      worker2
```

**En worker1:**
```bash
sudo nano /etc/hosts
```
```
127.0.0.1           localhost
100.107.94.42       worker1
100.126.227.85      tokito
100.112.192.60      worker2
```

**En worker2:**
```bash
sudo nano /etc/hosts
```
```
127.0.0.1           localhost
100.112.192.60      worker2
100.126.227.85      tokito
100.107.94.42       worker1
```

Verificar resolución de hostnames:
```bash
ping -c 2 tokito
ping -c 2 worker1
ping -c 2 worker2
```

---

## Parte 5 — Configurar SSH sin contraseña

Hadoop usa SSH para iniciar daemons remotamente. Debe funcionar desde tokito hacia todos los workers.

### Problema: Debian 13 bloquea autenticación por contraseña

Habilitar temporalmente para el primer ssh-copy-id:

```bash
# En worker2 como root
nano /etc/ssh/sshd_config
# Cambiar: PasswordAuthentication no → PasswordAuthentication yes
systemctl restart sshd
```

### Agregar la clave de tokito a worker2 manualmente

```bash
# En tokito, obtener la clave pública del usuario hadoop
cat /home/hadoop/.ssh/id_rsa.pub
# Copiar el texto completo
```

```bash
# En worker2 como root, agregar la clave al usuario hadoop
tail -1 /root/.ssh/authorized_keys >> /home/hadoop/.ssh/authorized_keys
chown hadoop:hadoop /home/hadoop/.ssh/authorized_keys
chmod 600 /home/hadoop/.ssh/authorized_keys
```

### Configurar SSH bidireccional

```bash
# En tokito como usuario hadoop
ssh-copy-id hadoop@worker2

# En worker2 como usuario hadoop
su - hadoop
ssh-copy-id hadoop@tokito
```

### Volver a deshabilitar autenticación por contraseña en worker2

```bash
# En worker2 como root
nano /etc/ssh/sshd_config
# Cambiar: PasswordAuthentication yes → PasswordAuthentication no
systemctl restart sshd
```

### Verificar

```bash
# Desde tokito
ssh hadoop@worker2   # debe entrar sin contraseña
# Desde worker2
ssh hadoop@tokito    # debe entrar sin contraseña
```

---

## Parte 6 — Instalación de Java y Hadoop en worker2

En lugar de copiar desde tokito (lento por internet), se descarga directamente en el Droplet.

### Instalar Java 8 (Temurin) — descarga directa

```bash
# En worker2 como root
cd /tmp
wget https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u412-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u412b08.tar.gz
tar -xzf OpenJDK8U-jdk_x64_linux_hotspot_8u412b08.tar.gz
mv jdk8u412-b08 /opt/hadoop/jdk
chown -R hadoop:hadoop /opt/hadoop/jdk
```

Verificar:
```bash
/opt/hadoop/jdk/bin/java -version
# OpenJDK version "1.8.0_412"
```

### Instalar Hadoop 3.3.6 — descarga directa

```bash
# En worker2 como root
cd /tmp
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
cp -r hadoop-3.3.6/* /opt/hadoop/
chown -R hadoop:hadoop /opt/hadoop
```

### Configurar variables de entorno en worker2

```bash
# En worker2 como usuario hadoop
nano ~/.bashrc
```

Agregar al final:
```bash
export JAVA_HOME=/opt/hadoop/jdk
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:/home/hadoop/.local/bin
```

```bash
source ~/.bashrc
```

### Configurar JAVA_HOME en hadoop-env.sh

```bash
nano /opt/hadoop/etc/hadoop/hadoop-env.sh
# Descomentar y cambiar:
export JAVA_HOME=/opt/hadoop/jdk
```

Verificar:
```bash
hadoop version
# Hadoop 3.3.6
```

---

## Parte 7 — Actualizar configuración del clúster en tokito

### Actualizar archivo workers

```bash
# En tokito como usuario hadoop
nano /opt/hadoop/etc/hadoop/workers
```
```
worker1
worker2
```

### Verificar hdfs-site.xml (replicación)

```bash
nano /opt/hadoop/etc/hadoop/hdfs-site.xml
```
```xml
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/hadoop/hadoopdata/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/hadoop/hadoopdata/datanode</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
</configuration>
```

> `dfs.replication=2` — un bloque en cada worker. Ajustar si hay más workers activos.

---

## Parte 8 — Copiar configuración y levantar worker2

### Copiar configs desde tokito a worker2

```bash
# En tokito como usuario hadoop
scp /opt/hadoop/etc/hadoop/core-site.xml    hadoop@worker2:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/hdfs-site.xml    hadoop@worker2:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/yarn-site.xml    hadoop@worker2:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/mapred-site.xml  hadoop@worker2:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/workers          hadoop@worker2:/opt/hadoop/etc/hadoop/
```

### Copiar configuración actualizada a worker1

```bash
scp /opt/hadoop/etc/hadoop/core-site.xml    hadoop@worker1:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/hdfs-site.xml    hadoop@worker1:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/yarn-site.xml    hadoop@worker1:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/mapred-site.xml  hadoop@worker1:/opt/hadoop/etc/hadoop/
scp /opt/hadoop/etc/hadoop/workers          hadoop@worker1:/opt/hadoop/etc/hadoop/
```

### Crear directorios de datos en worker2

```bash
ssh hadoop@worker2 "mkdir -p /home/hadoop/hadoopdata/{namenode,datanode,tmp}"
```

### Arrancar Hadoop en tokito (si no está corriendo)

```bash
start-dfs.sh
start-yarn.sh
```

### Iniciar daemons en worker2 sin detener el clúster

```bash
ssh hadoop@worker2 "/opt/hadoop/sbin/hadoop-daemon.sh start datanode"
ssh hadoop@worker2 "/opt/hadoop/sbin/yarn-daemon.sh start nodemanager"
```

### Reiniciar daemons en worker1 con nueva configuración

```bash
ssh hadoop@worker1 "/opt/hadoop/sbin/hadoop-daemon.sh stop datanode"
ssh hadoop@worker1 "/opt/hadoop/sbin/yarn-daemon.sh stop nodemanager"
ssh hadoop@worker1 "/opt/hadoop/sbin/hadoop-daemon.sh start datanode"
ssh hadoop@worker1 "/opt/hadoop/sbin/yarn-daemon.sh start nodemanager"
```

### Verificar el clúster completo

```bash
# Verificar daemons
jps                                              # tokito: NameNode, SecondaryNameNode, ResourceManager
ssh hadoop@worker1 "/opt/hadoop/jdk/bin/jps"    # DataNode, NodeManager
ssh hadoop@worker2 "/opt/hadoop/jdk/bin/jps"    # DataNode, NodeManager

# Verificar HDFS y YARN
hdfs dfsadmin -report
yarn node -list
```

**Salida esperada de hdfs dfsadmin -report:**
```
Live datanodes (2):
Name: 100.107.94.42:9866 (worker1)   ← VM local (23.27 GB)
Name: 100.112.192.60:9866 (worker2)  ← Droplet DO (157.28 GB)
```

**Salida esperada de yarn node -list:**
```
Total Nodes: 2
worker2:40081   RUNNING
worker1:41533   RUNNING
```

### Rebalancear bloques existentes

```bash
hdfs balancer
# "The cluster is balanced. Exiting..." → OK
```

---

## Parte 9 — Configuración de PySpark en worker2

### Instalar setuptools (requerido para Python 3.13)

```bash
ssh hadoop@worker2 "sudo apt install python3-setuptools -y"
```

### Copiar PySpark desde tokito

```bash
# En tokito como usuario hadoop
ssh hadoop@worker2 "mkdir -p /home/hadoop/.local/lib/python3.13/site-packages /home/hadoop/.local/bin"

scp -r /home/hadoop/.local/lib/python3.13/site-packages/pyspark \
    hadoop@worker2:/home/hadoop/.local/lib/python3.13/site-packages/

scp -r /home/hadoop/.local/lib/python3.13/site-packages/py4j \
    hadoop@worker2:/home/hadoop/.local/lib/python3.13/site-packages/

scp /home/hadoop/.local/bin/* hadoop@worker2:/home/hadoop/.local/bin/
```

Verificar:
```bash
ssh hadoop@worker2 "python3 -c 'import pyspark; print(pyspark.__version__)'"
# 3.5.0
```

---

## Parte 10 — JARs de Spark en HDFS

Subir los JARs de Spark a HDFS una sola vez para arranques más rápidos:

```bash
hdfs dfs -mkdir -p /spark-jars
hdfs dfs -put /home/hadoop/.local/lib/python3.13/site-packages/pyspark/jars/* /spark-jars/
```

> Esto puede tardar varios minutos por la replicación a worker2 vía internet. Se hace **una sola vez**.

### Configuración base de SparkSession con JARs en HDFS

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
    .appName('NombreApp')
    .master('yarn')
    .config('spark.submit.deployMode', 'client')
    .config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')
    .config('spark.driver.memory', '1g')
    .config('spark.executor.memory', '1g')
    .config('spark.executor.cores', '1')
    .config('spark.executor.instances', '2')
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.adaptive.enabled', 'true')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')
```

> `spark.executor.instances = 2` → un executor en worker1 y otro en worker2.

---

## Parte 11 — Estructura de Carpetas del Proyecto

### Sistema local — tokito

```
/home/hadoop/
├── hadoopdata/              ← datos internos HDFS/YARN (no tocar)
├── spark/                   ← scripts Spark anteriores
└── online_retail/
    ├── data/                ← archivos temporales locales
    ├── scripts/
    │   ├── 01_ingesta.py
    │   ├── 02_etl.py
    │   ├── 03_kpis.py
    │   ├── 04_inventario.py
    │   ├── 05_ml.py
    │   └── pipeline_completo.py
    └── dashboard/
        ├── app.py           ← aplicación Streamlit
        └── data/            ← CSVs exportados para el dashboard
            ├── top_productos.csv
            ├── por_pais.csv
            ├── flujo_mensual.csv
            ├── rfm.csv
            └── alertas_stock.csv
```

### HDFS — hdfs://tokito:8020/

```
hdfs:///
├── spark-jars/              ← JARs de Spark (subidos una vez)
├── online_retail/
│   ├── raw/
│   │   └── online_retail.csv           ← dataset original (1,067,371 filas)
│   ├── processed/
│   │   └── data_limpia/                ← Parquet limpio (805,549 filas)
│   └── output/
│       ├── kpis/
│       │   ├── top_productos/
│       │   ├── por_pais/
│       │   └── flujo_mensual/
│       ├── inventario/
│       │   └── alertas_stock/
│       └── predicciones/
│           └── rfm_segmentado/
└── [datasets anteriores: taxi_data/, raw_data/, spark_output/]
```

---

## Parte 12 — Dataset: Online Retail II UCI

### Información del dataset

| Característica | Valor |
|----------------|-------|
| Fuente | UCI Machine Learning Repository |
| URL descarga | `https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip` |
| Formato | Excel (.xlsx) — 2 hojas |
| Filas totales | 1,067,371 |
| Filas limpias | 805,549 (75.5%) |
| Periodo | Diciembre 2009 — Diciembre 2011 |
| Países | 37 |

### Columnas del dataset

| Columna | Tipo | Descripción |
|---------|------|-------------|
| Invoice | String | Número de factura (C = cancelada) |
| StockCode | String | Código de producto |
| Description | String | Descripción del producto |
| Quantity | Integer | Unidades por transacción |
| InvoiceDate | Timestamp | Fecha y hora de la factura |
| Price | Double | Precio unitario en £ |
| Customer ID | Double | ID del cliente (puede ser nulo) |
| Country | String | País del cliente |

### Descarga y preparación

```bash
# Descargar
cd /home/hadoop/online_retail/data
wget https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip
unzip "online+retail+ii.zip"

# Instalar dependencia para leer xlsx
pip install openpyxl --break-system-packages

# Convertir ambas hojas a un único CSV
python3 -c "
import pandas as pd
print('Leyendo hoja 1...')
df1 = pd.read_excel('online_retail_II.xlsx', sheet_name='Year 2009-2010')
print('Leyendo hoja 2...')
df2 = pd.read_excel('online_retail_II.xlsx', sheet_name='Year 2010-2011')
df = pd.concat([df1, df2], ignore_index=True)
df.to_csv('online_retail.csv', index=False)
print(f'Total filas: {len(df):,}')
"

# Subir a HDFS
hdfs dfs -put online_retail.csv /online_retail/raw/
```

---

## Parte 13 — Pipeline Completo de Análisis

### Script: /home/hadoop/online_retail/scripts/pipeline_completo.py

```python
import os
os.environ['JAVA_HOME']        = '/opt/hadoop/jdk'
os.environ['HADOOP_HOME']      = '/opt/hadoop'
os.environ['HADOOP_CONF_DIR']  = '/opt/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']    = '/opt/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']   = 'python3'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sum, count, avg, round, desc, countDistinct,
    datediff, lit, max as spark_max, when, stddev, year, month, dayofmonth, hour, dayofweek)
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
    SELECT ROUND(AVG(ticket_total), 2) AS Ticket_Promedio,
           ROUND(MIN(ticket_total), 2) AS Ticket_Minimo,
           ROUND(MAX(ticket_total), 2) AS Ticket_Maximo
    FROM (SELECT Invoice, SUM(TotalAmount) AS ticket_total FROM ventas GROUP BY Invoice)
""")
print('\n── KPI 1: Ticket Promedio ──')
ticket.show()

top_productos = spark.sql("""
    SELECT StockCode, Description,
           SUM(Quantity) AS UnidadesVendidas,
           ROUND(SUM(TotalAmount), 2) AS IngresoTotal,
           ROUND(AVG(Quantity), 1) AS PromUnidadesPedido
    FROM ventas GROUP BY StockCode, Description
    ORDER BY UnidadesVendidas DESC LIMIT 10
""")
print('── KPI 2: Top 10 Productos ──')
top_productos.show(truncate=False)

por_pais = spark.sql("""
    SELECT Country,
           ROUND(SUM(TotalAmount), 2) AS IngresoTotal,
           COUNT(DISTINCT CustomerID) AS ClientesUnicos,
           COUNT(DISTINCT Invoice) AS Facturas,
           ROUND(AVG(TotalAmount), 2) AS TicketPromedio
    FROM ventas GROUP BY Country ORDER BY IngresoTotal DESC LIMIT 10
""")
print('── KPI 3: Ingresos por País ──')
por_pais.show()

flujo_mensual = spark.sql("""
    SELECT Year, Month,
           ROUND(SUM(TotalAmount), 2) AS IngresosMes,
           COUNT(DISTINCT Invoice) AS Facturas,
           COUNT(DISTINCT CustomerID) AS ClientesActivos
    FROM ventas GROUP BY Year, Month ORDER BY Year, Month
""")
print('── KPI 4: Flujo de Caja Mensual ──')
flujo_mensual.show(24)

segmentos = spark.sql("""
    SELECT CASE WHEN gasto_total > 5000 THEN 'B2B' ELSE 'B2C' END AS Segmento,
           COUNT(*) AS NumClientes,
           ROUND(AVG(gasto_total), 2) AS GastoPromedio,
           ROUND(AVG(num_facturas), 1) AS FacturasPromedio
    FROM (SELECT CustomerID, SUM(TotalAmount) AS gasto_total,
                 COUNT(DISTINCT Invoice) AS num_facturas
          FROM ventas GROUP BY CustomerID)
    GROUP BY Segmento
""")
print('── KPI 5: B2B vs B2C ──')
segmentos.show()

alertas = spark.sql("""
    SELECT StockCode, Description,
           ROUND(STDDEV(Quantity), 2) AS Desviacion,
           ROUND(AVG(Quantity), 2) AS PromedioUnidades,
           COUNT(Invoice) AS NumPedidos,
           ROUND(STDDEV(Quantity) / AVG(Quantity), 2) AS CoefVariacion
    FROM ventas GROUP BY StockCode, Description
    HAVING COUNT(Invoice) > 20 AND STDDEV(Quantity) IS NOT NULL
    ORDER BY CoefVariacion DESC LIMIT 10
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
    round(avg('Monetary'), 2).alias('Monetary_Prom')
).orderBy(desc('Monetary_Prom')).show()

print('── Distribución Tipos de Cliente ──')
rfm_final.groupBy('TipoCliente').count().orderBy(desc('count')).show()

# ══════════════════════════════════════════════════════════════
# FASE 5 — ESCRITURA HDFS
# ══════════════════════════════════════════════════════════════
print('\n[5/6] ESCRITURA — Guardando en HDFS...')
rfm_final.select('CustomerID', 'Recency', 'Frequency', 'Monetary', 'prediction', 'TipoCliente') \
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

### Ejecutar el pipeline

```bash
python3 /home/hadoop/online_retail/scripts/pipeline_completo.py
```

---

## Parte 14 — Resultados del Análisis

### KPI 1 — Ticket Promedio por Factura

| Métrica | Valor |
|---------|-------|
| Ticket Promedio | £479.95 |
| Ticket Mínimo | £0.38 |
| Ticket Máximo | £168,469.60 |

> El ticket alto confirma presencia dominante de clientes B2B mayoristas.

### KPI 2 — Top 10 Productos por Rotación

| Producto | Unidades Vendidas | Ingreso Total |
|---------|------------------|--------------|
| World War 2 Gliders | 109,169 | £24,905 |
| White Hanging Heart T-Light | 93,640 | £252,072 |
| Paper Craft Little Birdie | 80,995 | £168,469 |
| Assorted Colour Bird Ornament | 79,913 | £127,074 |
| Medium Ceramic Storage Jar | 77,916 | £81,416 |

> "Paper Craft Little Birdie": 80,995 unidades en UN solo día — pedido B2B masivo que distorsiona el indicador.

### KPI 3 — Ingresos por País

| País | Ingreso Total | Clientes | Ticket Prom. |
|------|--------------|----------|-------------|
| United Kingdom | £14,723,147 | 5,350 | £20.30 |
| EIRE | £621,631 | 5 | £39.49 |
| Netherlands | £554,232 | 22 | £108.93 |
| Germany | £431,262 | 107 | £25.83 |
| France | £355,257 | 95 | £25.72 |

> EIRE (Irlanda): 5 clientes generan £621K → clientes B2B de alto volumen.

### KPI 4 — Flujo de Caja Mensual

Pico de ingresos en **noviembre y octubre** de ambos años — comportamiento estacional marcado previo a Navidad. Diciembre 2011 no está completo en el dataset (datos hasta el 9 de diciembre).

### KPI 5 — Segmentación B2B vs B2C

| Segmento | Clientes | Gasto Promedio | Facturas Prom. |
|----------|----------|---------------|----------------|
| B2C (≤ £5,000) | 5,208 | £1,146 | 3.8 |
| B2B (> £5,000) | 670 | £17,573 | 25.3 |

> B2B representa solo el 11.4% de clientes pero genera la mayor parte del ingreso.

### KPI 6 — Alertas Riesgo Quiebre de Stock

| Producto | Coef. Variación | Riesgo |
|---------|----------------|--------|
| Medium Ceramic Storage Jar | 13.4 | 🔴 Crítico |
| Pink Paper Parasol | 9.98 | 🔴 Alto |
| Blue Paisley Notebook | 9.37 | 🟠 Alto |

> Coeficiente de variación > 5 indica pedidos muy irregulares — alto riesgo de quiebre de stock.

### Segmentación RFM — Perfiles de Clientes (KMeans, k=4)

| Segmento | Clientes | Recency (días) | Frequency | Monetary (£) |
|----------|---------|----------------|-----------|-------------|
| Mega VIP | 9 | 3 | 199 facturas | £275,656 |
| VIP Activo | 138 | 29 | 50 facturas | £32,655 |
| Regular | 3,755 | 69 | 6.4 facturas | £2,467 |
| Inactivo | 1,976 | 465 | 2.2 facturas | £754 |

### Distribución de Tipos de Cliente

| Tipo | Clientes | % |
|------|---------|---|
| Perdido (> 90 días) | 2,960 | 50.4% |
| Activo Reciente (< 30 días) | 1,427 | 24.3% |
| En Riesgo (30-90 días) | 1,224 | 20.8% |
| VIP (Monetary > £10,000) | 267 | 4.5% |

> El 50.4% de clientes están perdidos — oportunidad crítica de campaña de reactivación.

---

## Parte 15 — Dashboard Streamlit

### Instalación (solo una vez)

```bash
pip install streamlit plotly --break-system-packages
```

### Script: /home/hadoop/online_retail/dashboard/app.py

El dashboard incluye:
- **4 métricas principales** en la cabecera
- **Flujo de caja mensual** — gráfico de área interactivo
- **B2B vs B2C** — gráfico de dona
- **Top 10 productos** — barras horizontales por rotación
- **Ingresos por país** — barras por mercado
- **Segmentación RFM** — pie chart de tipos de cliente
- **Alertas de quiebre** — barras de riesgo
- **Tabla VIP** — top 15 clientes de mayor valor

### Ejecutar el dashboard

```bash
streamlit run /home/hadoop/online_retail/dashboard/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0
```

### Acceder desde Windows (túnel SSH)

```powershell
ssh -L 8501:localhost:8501 hadoop@100.126.227.85
```

Abrir en el navegador: **http://localhost:8501**

---

## Parte 16 — Comandos de Operación del Clúster

### Arrancar el clúster

```bash
# En tokito como usuario hadoop — siempre en este orden
start-dfs.sh
start-yarn.sh

# Verificar
jps
# NameNode, SecondaryNameNode, ResourceManager
```

### Detener el clúster

```bash
stop-yarn.sh
stop-dfs.sh
```

### Verificar estado completo

```bash
# Daemons
jps                                           # master
ssh hadoop@worker1 "/opt/hadoop/jdk/bin/jps" # worker1
ssh hadoop@worker2 "/opt/hadoop/jdk/bin/jps" # worker2

# HDFS y YARN
hdfs dfsadmin -report
yarn node -list

# HDFS
hdfs dfs -ls /
hdfs dfs -ls /online_retail/
```

### Interfaces web

| Interfaz | URL | Descripción |
|----------|-----|-------------|
| HDFS NameNode | http://tokito:9870 | DataNodes, bloques, replicación |
| YARN ResourceManager | http://tokito:8088 | Jobs, containers por nodo |
| Spark UI | http://tokito:4040 | Solo activo durante ejecución de Spark |

---

## Parte 17 — Errores Comunes y Soluciones

| Error | Causa | Solución |
|-------|-------|----------|
| `Permission denied (publickey)` al ssh-copy-id | Debian 13 bloquea auth por contraseña | Habilitar `PasswordAuthentication yes` temporalmente en sshd_config |
| `droplet-agent is not running` | Agente de DO no instalado | `curl -sSL https://repos-droplet.digitalocean.com/install.sh \| sudo bash` |
| `Slow ReadProcessor` en HDFS | Replicación a worker2 vía internet lenta | Normal — esperar a que termine. Para uploads futuros usar `-Ddfs.replication=1` |
| `JAVA_HOME is not set` | Variable de entorno no configurada | Agregar `export JAVA_HOME=/opt/hadoop/jdk` en `~/.bashrc` y `hadoop-env.sh` |
| Spark se cuelga en inicio | JARs subiéndose cada vez | Subir JARs a HDFS y usar `spark.yarn.jars` |
| `falling back to uploading libraries` | `spark.yarn.jars` no configurado | Agregar `.config('spark.yarn.jars', 'hdfs://tokito:8020/spark-jars/*')` |
| `Cache Used%: 100%` en workers | No hay cache HDFS configurado | Normal — no afecta funcionamiento |
| Balancer dice "cluster is balanced" inmediatamente | Diferencia entre workers < 10% (umbral) | Normal — el balance está dentro del margen aceptable |

---

## Parte 18 — Notas de Arquitectura

### Por qué Tailscale en lugar de exposición directa de puertos

Hadoop y Spark usan docenas de puertos dinámicos para comunicación interna (DataNode, NodeManager, ejecutores Spark). Abrir todos estos puertos en el firewall del router sería inseguro y complejo. Tailscale crea una VPN mesh que hace que todos los nodos se vean en una red privada virtual `100.x.x.x` sin exponer puertos al internet público.

### Latencia worker2 vs worker1

Worker1 (VM local) tiene latencia ~1ms con tokito. Worker2 (DigitalOcean NYC) tiene latencia ~150-200ms vía Tailscale. Esto impacta principalmente en:
- Escrituras HDFS con replicación (cada bloque va a ambos workers)
- Transferencia de datos entre executors de Spark

Para minimizar el impacto: subir datos grandes con `dfs.replication=1` primero y luego aumentar con `hdfs dfs -setrep 2 /ruta`.

### Diferencias: Colab vs Clúster Real (Spark)

| Aspecto | Google Colab | Clúster Real |
|---------|-------------|-------------|
| Master | `local[*]` | `yarn` |
| Almacenamiento | `/tmp/` local | `hdfs://tokito:8020/...` |
| Java | 17/21 con flags `--add-opens` | Java 8 sin flags |
| Workers | 1 máquina virtual | 2 workers distribuidos |
| JARs | Se suben cada vez | Pre-cargados en HDFS |
| Ejecución | Celdas Jupyter | `python3 script.py` |
