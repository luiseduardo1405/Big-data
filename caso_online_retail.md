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
2. **KPIs de negocio** — ticket promedio, rotación de productos, retención de clientes
3. **Control de inventario** — detectar quiebres de stock y productos de alto riesgo
4. **Segmentación de clientes** — distinguir comportamiento B2B (mayorista) de B2C (minorista)
5. **Flujo de caja** — entender los ciclos de ingreso diario/mensual del negocio

> Esta arquitectura de análisis es replicable para sistemas de gestión financiera de PyMEs reales.

---

## Pipeline de Procesamiento

```
Ingesta (CSV crudo)
    ↓
ETL (limpieza + features de fecha)
    ↓
Spark SQL (KPIs agregados)
    ↓
MLlib (segmentación RFM con KMeans)
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

| Métrica | Valor |
|---------|-------|
| Ticket Promedio | £479.95 |
| Ticket Mínimo | £0.38 |
| Ticket Máximo | £168,469.60 |

**Insight:** Un ticket promedio tan alto (vs. ~£20-50 esperado en retail minorista típico) confirma una fuerte presencia de pedidos mayoristas (B2B) mezclados con compras individuales pequeñas.

---

### 2. Rotación de Productos (Top Sellers)

| Producto | Unidades Vendidas | Ingreso Total |
|---------|-------------------|---------------|
| World War 2 Gliders | 109,169 | £24,905 |
| White Hanging Heart T-Light Holder | 93,640 | £252,072 |
| Paper Craft, Little Birdie | 80,995 | £168,469 |
| Assorted Colour Bird Ornament | 79,913 | £127,074 |
| Medium Ceramic Top Storage Jar | 77,916 | £81,416 |

**Insight:** El producto #1 en unidades (Gliders) genera 10x menos ingreso que el #2 (T-Light Holder) — la rotación alta no siempre significa mayor rentabilidad.

---

### 3. Ingresos por País (Expansión Geográfica)

| País | Ingreso Total | Clientes Únicos | Ticket Promedio |
|------|---------------|------------------|------------------|
| United Kingdom | £14,723,147 | 5,350 | £20.30 |
| EIRE (Irlanda) | £621,631 | 5 | £39.49 |
| Netherlands | £554,232 | 22 | £108.93 |
| Germany | £431,262 | 107 | £25.83 |
| France | £355,257 | 95 | £25.72 |

**Insight crítico:** EIRE genera £621K con solo **5 clientes** — son cuentas B2B masivas, no consumidores individuales. Mismo patrón en Netherlands (£554K con 22 clientes).

---

### 4. Flujo de Caja Mensual

**Patrón estacional identificado:**
- Picos consistentes en **septiembre, octubre y noviembre** de ambos años — preparación para temporada navideña
- El mejor mes de cada año supera los £1,000,000 en ingresos
- Caída en enero/febrero — periodo post-navideño típico del retail

---

### 5. Segmentación B2B vs B2C

| Segmento | Definición | Clientes | % del total | Gasto Promedio | Facturas Prom. |
|----------|-----------|----------|--------------|-----------------|------------------|
| **B2C** | Gasto ≤ £5,000 | 5,208 | 88.6% | £1,146 | 3.8 |
| **B2B** | Gasto > £5,000 | 670 | 11.4% | £17,573 | 25.3 |

**Insight estratégico:** Solo el 11.4% de clientes (B2B) genera la mayoría de los ingresos totales del negocio con 6.6x más facturas en promedio que el cliente B2C típico.

---

### 6. Alertas de Riesgo de Quiebre de Stock

Calculado mediante **Coeficiente de Variación** (Desviación Estándar / Promedio) en las cantidades pedidas por producto:

| Producto | Coef. Variación | Nivel de Riesgo |
|---------|------------------|------------------|
| Medium Ceramic Top Storage Jar | 13.40 | 🔴 Crítico |
| Pink Paper Parasol | 9.98 | 🔴 Crítico |
| Blue Paisley Notebook | 9.37 | 🟠 Alto |
| Chrysanthemum Notebook | 8.96 | 🟠 Alto |
| Pack of 12 Woodland Tissues | 8.54 | 🟠 Alto |

**Insight:** Un coeficiente > 5 indica que los pedidos de ese producto son extremadamente irregulares (a veces 0, a veces miles de unidades) — alto riesgo de sobre-stock o quiebre según el momento.

---

## Segmentación de Clientes con Machine Learning (RFM + KMeans)

### Metodología RFM

| Variable | Significado | Cálculo |
|----------|-------------|---------|
| **R**ecency | Días desde la última compra | `fecha_referencia - última_compra` |
| **F**requency | Número de facturas distintas | `COUNT(DISTINCT Invoice)` |
| **M**onetary | Gasto total acumulado | `SUM(TotalAmount)` |

### Clustering con KMeans (k=4)

| Segmento | Clientes | Recency (días) | Frequency | Monetary (£) | Perfil |
|----------|----------|-----------------|-----------|----------------|--------|
| Mega VIP | 9 | 3 | 199 facturas | £275,656 | Cuentas corporativas masivas |
| VIP Activo | 138 | 29 | 50 facturas | £32,655 | Mayoristas frecuentes |
| Regular | 3,755 | 69 | 6.4 facturas | £2,467 | Compradores habituales |
| Inactivo | 1,976 | 465 | 2.2 facturas | £754 | Clientes perdidos hace +1 año |

### Clasificación de Negocio (basada en reglas)

| Tipo de Cliente | Criterio | Clientes | % |
|------------------|----------|----------|---|
| **VIP - Alto Valor** | Monetary > £10,000 | 267 | 4.5% |
| **Activo Reciente** | Recency < 30 días | 1,427 | 24.3% |
| **En Riesgo** | Recency 30-90 días | 1,224 | 20.8% |
| **Perdido** | Recency > 90 días | 2,960 | 50.4% |

**Insight crítico de negocio:** **El 50.4% de la base de clientes está perdida** (más de 90 días sin comprar). Esto representa una oportunidad clara para una campaña de reactivación dirigida, especialmente porque muchos de estos clientes "perdidos" alguna vez tuvieron compras significativas.

---

## Conclusiones del Caso

1. **Dependencia B2B:** El negocio depende fuertemente de un núcleo pequeño de clientes mayoristas (4.5% de la base genera la mayor parte del ingreso).

2. **Estacionalidad fuerte:** Las ventas están claramente ancladas al ciclo navideño (Q4), sugiriendo necesidad de gestión de inventario anticipada en agosto-septiembre.

3. **Riesgo de inventario:** Productos con alta variabilidad en demanda (coeficiente > 8) requieren un modelo de reposición diferente al estándar (buffer stock dinámico en vez de fijo).

4. **Retención crítica:** La mitad de los clientes históricos están inactivos — el negocio pierde más clientes de los que retiene a largo plazo.

5. **Geografía concentrada:** El 85%+ del ingreso proviene de UK, pero los mercados con menos clientes (EIRE, Netherlands) muestran tickets promedio mucho más altos — posible oportunidad de expansión B2B internacional.

---

## Herramientas Utilizadas

| Herramienta | Uso |
|-------------|-----|
| **Apache Hadoop 3.3.6** | Almacenamiento distribuido (HDFS) |
| **Apache Spark 3.5.0** | Procesamiento distribuido (SQL + MLlib) |
| **PySpark** | Interfaz Python para Spark |
| **KMeans (MLlib)** | Clustering no supervisado para RFM |
| **Streamlit + Plotly** | Dashboard interactivo de visualización |
| **Clúster distribuido** | 1 master + 2 workers (local + DigitalOcean) |
