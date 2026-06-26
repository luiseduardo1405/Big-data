# Caso de Estudio: Análisis Estratégico Comercial — Online Retail II

## Rol Asumido

**Analista de Estrategia Comercial** en una tienda minorista online (retailer B2B/B2C) con dos años completos de operación histórica.

---

## El Dataset

| Característica | Detalle |
|----------------|---------|
| **Nombre** | Online Retail II |
| **Fuente** | UCI Machine Learning Repository |
| **URL** | `https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip` |
| **Naturaleza** | Dataset público y libre, liberado para investigación |
| **Contenido** | Transacciones históricas reales: facturación, cantidades, fechas y precios |
| **Periodo** | Diciembre 2009 — Diciembre 2011 (2 años) |
| **Peso original** | ~45 MB en CSV |
| **Registros** | 1,067,371 transacciones brutas → 805,549 limpias |
| **Países** | 37 mercados |

### Columnas originales

| Columna | Descripción |
|---------|-------------|
| Invoice | Número de factura (prefijo "C" = cancelada) |
| StockCode | Código único de producto |
| Description | Descripción del producto |
| Quantity | Unidades vendidas |
| InvoiceDate | Fecha y hora de la transacción |
| Price | Precio unitario en £ |
| Customer ID | Identificador del cliente |
| Country | País del cliente |

---

## Objetivo del Dataset (Propósito Original)

Estudiar el comportamiento de compra de clientes minoristas, identificar patrones estacionales en las ventas, y proyectar la demanda futura a partir del historial transaccional.

---

## Mi Objetivo como Analista

Procesar la data cruda **como lo haría un ingeniero de datos en el mundo real**:

1. **Limpieza** — eliminar facturas anuladas, devoluciones y registros sin cliente identificado
2. **KPIs de negocio** — ticket promedio con dispersión, rotación real de productos, flujo de caja con estacionalidad
3. **Control de inventario** — detectar quiebres de stock y productos de alto riesgo con score ponderado
4. **Segmentación de clientes** — distinguir comportamiento B2B (mayorista) de B2C (minorista), con concentración de ingreso por segmento
5. **Flujo de caja** — entender los ciclos de ingreso mensual con crecimiento MoM e índice de estacionalidad

> Esta arquitectura de análisis es replicable para sistemas de gestión financiera de PyMEs reales.

---

## Pipeline de Procesamiento

```
Ingesta (CSV crudo)
    ↓
ETL (limpieza + features de fecha)
    ↓
Spark SQL (KPIs agregados con fórmulas estadísticas)
    ↓
MLlib (segmentación RFM con KMeans + RFM Score ntile)
    ↓
Escritura (Parquet en HDFS)
    ↓
Dashboard (Streamlit + CSVs)
```

### Fase de Limpieza (ETL)

| Filtro aplicado | Filas eliminadas | Razón |
|-----------------|------------------|-------|
| Facturas canceladas (prefijo "C") | 19,494 | Devoluciones, no son ventas reales |
| Customer ID nulo | 242,257 | Sin trazabilidad de cliente |
| Quantity o Price ≤ 0 | 71 | Datos corruptos |
| **Total eliminado** | **261,822 (24.5%)** | |
| **Filas finales válidas** | **805,549 (75.5%)** | |

---

## KPIs Estratégicos Identificados

### 1. Ticket Promedio por Factura

**Fórmula:** `Ticket_Promedio = AVG(SUM(TotalAmount) por factura)` — doble agregación que promedia el valor total de cada factura, no líneas individuales.

| Métrica | Valor |
|---------|-------|
| Ticket Promedio | £479.95 |
| Ticket Mínimo | £0.38 |
| Ticket Máximo | £168,469.60 |
| Desviación Estándar del Ticket | £1,374.99 |
| Coeficiente de Variación (CV) | 2.8648 (286.48%) |

**Insight:** El CV de 286% confirma que el promedio de £479.95 es estadísticamente engañoso — la dispersión es casi 3 veces mayor que el promedio. Coexisten facturas de £0.38 (compra individual pequeña) y de £168,469 (pedido mayorista masivo) en el mismo dataset. Un solo valor promedio no representa a ninguno de los dos perfiles.

---

### 2. Rotación de Productos (Top 10 por Unidades)

**Fórmulas aplicadas:**
- `VelocidadRotacion = SUM(Quantity) / COUNT(DISTINCT InvoiceDate)` — unidades por día activo de venta
- `Ingreso_por_Unidad = SUM(TotalAmount) / SUM(Quantity)` — precio promedio ponderado real
- `PctIngreso = IngresoTotal_producto / IngresoTotal_global × 100` — participación en ingreso global

| Producto | Unidades | Ingreso Total | Vel. Rotación | £/Unidad | % Ingreso |
|---------|----------|---------------|---------------|----------|-----------|
| World War 2 Gliders | 109,169 | £24,905.87 | 119.05/día | £0.23 | 0.14% |
| White Hanging Heart T-Light Holder | 93,640 | £252,072.46 | 19.33/día | £2.69 | 1.42% |
| Paper Craft, Little Birdie | 80,995 | £168,469.60 | 80,995/día | £2.08 | 0.95% |
| Assorted Colour Bird Ornament | 79,913 | £127,074.17 | 30.34/día | £1.59 | 0.72% |
| Medium Ceramic Top Storage Jar | 77,916 | £81,416.73 | 399.57/día | £1.04 | 0.46% |

**Insight:** El análisis multidimensional revela tres perfiles distintos que el ranking simple ocultaba:
- **Gliders:** #1 en volumen (119 unidades/día), pero £0.23 por unidad y solo 0.14% del ingreso global. Rotación engañosa — mueve mucho a casi ningún valor.
- **T-Light Holder:** Solo 19.33 unidades/día, pero £2.69 por unidad y 1.42% del ingreso. Eficiencia 10x superior.
- **Paper Craft:** VelocidadRotacion = 80,995 — fue un único pedido masivo en un día. No es rotación real, es un evento puntual B2B.

---

### 3. Ingresos por País (Expansión Geográfica)

**Fórmulas aplicadas:**
- `PctMercado = IngresoTotal_país / IngresoTotal_global × 100`
- `LTV_Pais = IngresoTotal_país / ClientesUnicos_país` — valor de vida promedio por cliente
- `Facturas_por_Cliente = COUNT(DISTINCT Invoice) / COUNT(DISTINCT CustomerID)`

| País | Ingreso Total | Clientes | % Mercado | LTV Promedio | Facturas/Cliente |
|------|---------------|----------|-----------|--------------|------------------|
| United Kingdom | £14,723,147.50 | 5,350 | 82.98% | £2,751.99 | 6.27 |
| EIRE (Irlanda) | £621,631.11 | 5 | 3.50% | £124,326.22 | 113.40 |
| Netherlands | £554,232.34 | 22 | 3.12% | £25,192.38 | 10.36 |
| Germany | £431,262.46 | 107 | 2.43% | £4,030.49 | 7.37 |
| France | £355,257.47 | 95 | 2.00% | £3,739.55 | 6.46 |

**Insight crítico:** El LTV por país expone la brecha B2B/B2C geográfica: EIRE tiene un LTV de £124,326 por cliente (45x el de UK) con 113 facturas promedio — son distribuidores mayoristas, no consumidores. Netherlands también: £25,192 con 10 facturas. Estos mercados pequeños en volumen de clientes son enormemente rentables por cliente individual. La oportunidad de expansión B2B no está en UK (ya saturado) sino en profundizar esos 5 clientes de EIRE y buscar perfiles similares en nuevos mercados.

---

### 4. Flujo de Caja Mensual

**Fórmulas aplicadas:**
- `MoM_Growth_Pct = (IngresosMes_t − IngresosMes_{t-1}) / IngresosMes_{t-1} × 100`
- `Indice_Estacionalidad = IngresosMes / Promedio_Mensual_Global × 100` (IS > 100 = temporada alta)

| Mes | Ingresos | MoM Growth | Índice Estacionalidad |
|-----|----------|------------|----------------------|
| 2010-09 | £831,615 | +37.63% | 117.17 |
| 2010-10 | £1,036,680 | +24.66% | 146.07 |
| 2010-11 | £1,172,336 | +13.09% | **165.18** (pico) |
| 2010-12 | £884,591 | -24.54% | 124.64 |
| 2011-01 | £569,445 | -35.63% | 80.23 |
| 2011-02 | £447,137 | -21.48% | **63.00** (valle) |
| 2011-09 | £952,838 | **+47.65%** | 134.25 |
| 2011-10 | £1,039,318 | +9.08% | 146.44 |
| 2011-11 | £1,161,817 | +11.79% | 163.70 |

**Insight:** El índice de estacionalidad cuantifica la brecha navideña: noviembre opera al 165% del promedio mensual, mientras febrero cae al 63%. La rampa de crecimiento más pronunciada ocurre en septiembre (+47.65% MoM en 2011), lo que indica que la preparación logística de inventario debe iniciarse en agosto. El patrón se replica consistentemente en ambos años, confirmando que no es ruido sino comportamiento estructural del negocio.

---

### 5. Segmentación B2B vs B2C

**Fórmulas aplicadas:**
- `PctIngreso = IngresoSegmento / IngresoTotal_global × 100`
- `VPC (Valor por Cliente) = IngresoSegmento / NumClientes`

| Segmento | Clientes | % Clientes | Gasto Prom. | Facturas Prom. | Ingreso Segmento | % Ingreso | VPC |
|----------|----------|-----------|-------------|----------------|-----------------|-----------|-----|
| **B2C** | 5,208 | 88.6% | £1,146.16 | 3.8 | £5,969,205.73 | 33.64% | £1,146.16 |
| **B2B** | 670 | 11.4% | £17,573.47 | 25.3 | £11,774,223.43 | 66.36% | £17,573.47 |

**Insight estratégico:** El desequilibrio es más extremo de lo que el gasto promedio sugería. El 11.4% de clientes B2B concentra el **66.36% del ingreso total** del negocio (£11.77M vs £5.97M del B2C). El VPC de un cliente B2B (£17,573) es 15.3x el de un cliente B2C (£1,146). Perder un cliente B2B equivale a perder 15 clientes B2C. Esto redefine la prioridad de retención: el riesgo operativo real está concentrado en 670 cuentas, no en 5,208.

---

### 6. Alertas de Riesgo de Quiebre de Stock

**Fórmulas aplicadas:**
- `CoefVariacion = STDDEV(Quantity) / AVG(Quantity)` — irregularidad de demanda
- `RiskScore = CoefVariacion × LOG(NumPedidos)` — pondera por impacto: alta variabilidad en productos de alta frecuencia es más crítico
- Clasificación: CV > 8 → Crítico | 5-8 → Alto | 2-5 → Medio | < 2 → Bajo

| Producto | CV | NumPedidos | RiskScore | Nivel |
|---------|-----|------------|-----------|-------|
| Medium Ceramic Top Storage Jar | 13.40 | 198 | **70.86** | Crítico |
| Rotating Silver Angels T-Light Hldr | 8.35 | 589 | 53.26 | Crítico |
| Pack of 12 Suki Tissues | 8.34 | 558 | 52.73 | Crítico |
| Pink Paper Parasol | 9.98 | 195 | 52.61 | Crítico |
| Pack of 12 Woodland Tissues | 8.54 | 418 | 51.53 | Crítico |

**Insight:** El RiskScore reordena las prioridades respecto al CV puro. "Rotating Silver Angels" tenía CV=8.35 (menor que Pink Paper Parasol con CV=9.98), pero su RiskScore es mayor (53.26 vs 52.61) porque se pide 589 veces — la demanda irregular afecta una cadena de reposición mucho más activa. Un producto pedido 5 veces con alta variabilidad es manejable; uno pedido 589 veces con la misma variabilidad es un riesgo operativo continuo.

---

## Segmentación de Clientes con Machine Learning (RFM + KMeans)

### Metodología RFM

| Variable | Significado | Cálculo |
|----------|-------------|---------|
| **R**ecency | Días desde la última compra | `datediff(fecha_max, MAX(InvoiceDate))` |
| **F**requency | Número de facturas distintas | `COUNT(DISTINCT Invoice)` |
| **M**onetary | Gasto total acumulado | `SUM(TotalAmount)` |

### RFM Score Compuesto

Cada dimensión se clasifica en quintiles (1-5) usando `ntile(5)`. Score 5 = mejor en esa dimensión.

- **R_score:** menor Recency → score 5 (compró más recientemente)
- **F_score:** mayor Frequency → score 5 (compra más seguido)
- **M_score:** mayor Monetary → score 5 (gasta más)
- **RFM_Score = R_score × 100 + F_score × 10 + M_score** (rango: 111 a 555)

Un cliente con RFM_Score = 555 es ideal (reciente, frecuente, alto gasto).

### Clustering con KMeans (k=4) + RFM Score Promedio

| Segmento | Clientes | Recency (días) | Frequency | Monetary (£) | RFM Score Prom. |
|----------|----------|-----------------|-----------|----------------|-----------------|
| Mega VIP (pred=2) | 9 | 3 | 199 facturas | £275,656.39 | **555.0** |
| VIP Activo (pred=3) | 138 | 29 | 50.6 facturas | £32,655.38 | 519.3 |
| Regular (pred=0) | 3,755 | 69 | 6.4 facturas | £2,467.71 | 416.0 |
| Inactivo (pred=1) | 1,976 | 465 | 2.2 facturas | £753.96 | **161.0** |

**Validación cruzada:** El RFM Score confirma la calidad del clustering. Los 9 clientes Mega VIP obtienen 555/555 — máximo posible en las tres dimensiones simultáneamente. Los Inactivos promedian 161, concentrado en scores bajos de Recency (no compran hace mucho). La separación entre segmentos es clara y no hay solapamiento en los scores.

### Clasificación de Negocio (basada en reglas sobre RFM)

| Tipo de Cliente | Criterio | Clientes | % |
|------------------|----------|----------|---|
| **VIP - Alto Valor** | Monetary > £10,000 | 267 | 4.5% |
| **Activo Reciente** | Recency < 30 días | 1,427 | 24.3% |
| **En Riesgo** | Recency 30-90 días | 1,224 | 20.8% |
| **Perdido** | Recency > 90 días | 2,960 | 50.4% |

**Insight crítico de negocio:** El 50.4% de la base de clientes está perdida (más de 90 días sin comprar). Combinado con el dato de B2B/B2C: si una fracción de esos 2,960 clientes perdidos pertenece al segmento B2B (gasto > £5,000), la pérdida de ingreso potencial es crítica. Una campaña de reactivación priorizada por RFM_Score y segmento B2B/B2C tendría el mayor retorno.

---

## Conclusiones del Caso

1. **Dependencia B2B extrema:** El 11.4% de clientes B2B concentra el 66.36% del ingreso total. El VPC B2B (£17,573) es 15.3x el B2C. La retención de esas 670 cuentas es el riesgo operativo más crítico del negocio.

2. **El promedio engaña:** CV del ticket = 286%. El ticket promedio de £479.95 no representa a ningún cliente real — conviven pedidos de £0.38 con pedidos de £168,469. Los análisis de precio y rentabilidad deben segmentarse por B2B/B2C antes de interpretar cualquier promedio.

3. **Estacionalidad cuantificada:** Noviembre opera al 165% del promedio mensual; febrero al 63%. La rampa de crecimiento crítica ocurre en septiembre (+47% MoM), lo que implica que la planificación de inventario debe ejecutarse en agosto.

4. **Riesgo de inventario ponderado:** El RiskScore corrige el ranking de productos en riesgo. No solo el CV importa — también la frecuencia de pedido. Los productos más críticos son los que combinan alta variabilidad con alta actividad de reposición (Rotating Silver Angels: CV=8.35 pero 589 pedidos).

5. **Geografía B2B oculta:** EIRE genera £124,326 por cliente con 113 facturas promedio. Es el mercado con mayor potencial de expansión B2B — replicar ese modelo de cuenta en nuevos países podría triplicar el ingreso internacional sin necesidad de ampliar la base de clientes B2C.

6. **Retención crítica:** 2,960 clientes perdidos (50.4% de la base). Priorizar reactivación por RFM_Score descendente dentro del segmento B2B perdido maximiza el retorno de cualquier campaña.

---

## Herramientas Utilizadas

| Herramienta | Uso |
|-------------|-----|
| **Apache Hadoop 3.3.6** | Almacenamiento distribuido (HDFS) |
| **Apache Spark 3.5.0** | Procesamiento distribuido (SQL + MLlib) |
| **PySpark** | Interfaz Python para Spark |
| **Spark SQL (CTEs + Window Functions)** | KPIs con LAG, ntile, SUM OVER para fórmulas estadísticas |
| **KMeans (MLlib)** | Clustering no supervisado para RFM |
| **StandardScaler (MLlib)** | Normalización z-score antes del clustering |
| **Streamlit + Plotly** | Dashboard interactivo de visualización |
| **Clúster distribuido** | 1 master + 2 workers (local + DigitalOcean) |
