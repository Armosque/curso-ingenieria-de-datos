# Módulo de SQL Avanzado con Olist  
## Guía para clase (docente y estudiantes)

Este documento describe cómo usar el dataset de **Olist** para enseñar y practicar conceptos de **SQL avanzado** en Postgres.

El foco no es solo “escribir queries”, sino aprender a:
- Modelar bien las preguntas de negocio.
- Escribir consultas legibles y mantenibles.
- Usar funciones ventana, CTEs y buenas prácticas.
- Reconocer y evitar antipatrones comunes de SQL.


---

## 1. Requisitos previos

Antes de usar este módulo, se asume que el estudiante ya puede:

- Escribir consultas básicas con:
  - `SELECT`, `FROM`, `WHERE`.
  - `JOIN` simples.
  - `GROUP BY`, `HAVING`, `ORDER BY`.
- Entender la idea de claves primarias y foráneas.
- Conectarse a Postgres (psql, DBeaver, DataGrip, etc.).

Para el módulo avanzado se requiere además:

- Una base de datos `olist` en Postgres con las tablas del dataset Olist cargadas.
- Opcional: Docker y el entorno del curso ya preparado.


---

## 2. Dataset Olist: vista rápida

Tablas principales que usaremos:

- **olist_orders**
  - Estado, fechas clave, cliente asociado.
- **olist_customers**
  - Datos de localización del cliente.
- **olist_order_items**
  - Ítems de cada orden: producto, vendedor, precio, flete.
- **olist_order_payments**
  - Pagos de cada orden: tipo de pago, valor, cuotas.
- **olist_order_reviews**
  - Reviews: `review_score`, comentarios, timestamps.
- **olist_products**
  - Características de producto, categoría.
- **product_category_name_translation**
  - Traducción de categorías al inglés.
- **olist_sellers**
  - Datos de los vendedores.
- **olist_geolocation**
  - Información de geolocalización por código postal.

Suele ser útil presentar el esquema como:

- `olist_orders` como hecho central.
- `olist_order_items`, `olist_order_payments`, `olist_order_reviews` como “hechos satélite” por orden.
- `customers`, `sellers`, `products`, `geolocation` como dimensiones.


---

## 3. Plan de clases (SQL avanzado con Olist)

### Clase 1 – Joins avanzados y esquema tipo fact/dimension

**Objetivos**

- Repasar y profundizar en `JOIN` (INNER, LEFT, RIGHT).
- Entender `olist_orders` como tabla de hechos y `customers`, `products`, `sellers` como dimensiones.
- Practicar agregaciones multinivel (por estado, por mes, por categoría).

**Conceptos clave**

- Hecho vs dimensión (sin tanta teoría de DW).
- Duplicidad y cardinalidad en joins (uno a muchos, muchos a muchos).
- `COUNT(*)` vs `COUNT(DISTINCT ...)`.

**Ejemplo base**

```sql
SELECT
    c.customer_state,
    date_trunc('month', o.order_purchase_timestamp) AS month,
    COUNT(DISTINCT o.order_id)                     AS num_orders,
    SUM(oi.price + oi.freight_value)               AS revenue
FROM olist_orders o
JOIN olist_customers c
  ON c.customer_id = o.customer_id
JOIN olist_order_items oi
  ON oi.order_id = o.order_id
WHERE o.order_status IN ('shipped', 'delivered')
GROUP BY
    c.customer_state,
    date_trunc('month', o.order_purchase_timestamp)
ORDER BY
    c.customer_state,
    month;
```


---

### Clase 2 – Funciones ventana (window functions)

**Objetivos**

- Introducir `OVER (PARTITION BY ...)`.
- Trabajar con `ROW_NUMBER`, `RANK`, `DENSE_RANK`.
- Usar `SUM() OVER` y `AVG() OVER` para acumulados y proporciones.

**Conceptos clave**

- Diferencia entre agregaciones tradicionales (`GROUP BY`) y funciones ventana.
- Partición y orden dentro de la ventana.
- Casos de uso típicos: top N por grupo, acumulados, comparaciones con la fila anterior.

**Ejemplo: top 3 productos por revenue en cada categoría**

```sql
WITH product_revenue AS (
    SELECT
        p.product_id,
        COALESCE(t.product_category_name_english, p.product_category_name) AS category_name,
        SUM(oi.price + oi.freight_value) AS revenue
    FROM olist_order_items oi
    JOIN olist_products p
      ON p.product_id = oi.product_id
    LEFT JOIN product_category_name_translation t
      ON t.product_category_name = p.product_category_name
    GROUP BY
        p.product_id,
        COALESCE(t.product_category_name_english, p.product_category_name)
)
SELECT
    category_name,
    product_id,
    revenue,
    RANK() OVER (PARTITION BY category_name ORDER BY revenue DESC) AS rnk
FROM product_revenue
ORDER BY category_name, rnk;
```


---

### Clase 3 – CTEs y refactor de consultas complejas (Spaghetti Query)

**Objetivos**

- Reconocer una “Spaghetti Query” (antipatrón).
- Aprender a refactorizar usando CTEs (`WITH`) con responsabilidades claras.
- Separar filtros, enriquecimientos y agregaciones.

**Conceptos clave**

- CTEs como “vistas inline” con nombre.
- Reutilización de lógica de filtros o joins en varios reportes.
- Legibilidad sobre micro optimización prematura.

**Ejemplo de patrón didáctico**

1. Consultar órdenes filtradas por estado y rango de fechas en un CTE `filtered_orders`.
2. Identificar órdenes con pago por tarjeta en `card_orders`.
3. Identificar órdenes con flete positivo en `orders_with_freight`.
4. Combinar todo en `orders_enriched` con `customer_state`.
5. Agregar items y reviews en CTEs separados (`items_agg`, `reviews_agg`).
6. Hacer el `SELECT` final sobre esos bloques.


---

### Clase 4 – NULL, valores mágicos y agregaciones condicionales

**Objetivos**

- Entender el antipatrón *Fear of the Unknown* (miedo a los NULL).
- Aprender a usar `NULL` correctamente, sin valores mágicos.
- Practicar `COALESCE` y agregaciones condicionales con `CASE WHEN`.

**Conceptos clave**

- `NULL` como “desconocido o no aplicable”.
- `IS NULL` y `IS NOT NULL` en lugar de `= NULL`.
- `COALESCE` para etiquetar datos faltantes en la presentación.
- `SUM(CASE WHEN ...)` como patrón para conteos condicionales.

**Ejemplos base**

Pedidos entregados tarde por estado:

```sql
SELECT
    c.customer_state,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
             AND o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN 1 ELSE 0
        END
    ) AS late_orders
FROM olist_orders o
JOIN olist_customers c ON c.customer_id = o.customer_id
GROUP BY c.customer_state;
```

Manejo de `payment_type` faltante:

```sql
SELECT
    COALESCE(payment_type, 'UNKNOWN') AS payment_type_label,
    COUNT(*) AS num_orders
FROM olist_order_payments
GROUP BY COALESCE(payment_type, 'UNKNOWN')
ORDER BY num_orders DESC;
```


---

### Clase 5 – Operaciones de conjuntos y cohortes de clientes

**Objetivos**

- Trabajar con `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`.
- Construir cohortes: grupos de clientes por períodos o comportamientos.
- Analizar retención y abandono básico con solo SQL.

**Conceptos clave**

- Diferencias entre `UNION` y `UNION ALL`.
- Uso de `EXCEPT` para encontrar “clientes que antes sí, ahora no”.
- Construcción de cohortes por primera compra.

**Ejemplo: clientes que compraron en 2017 pero no en 2018**

```sql
WITH customers_2017 AS (
    SELECT DISTINCT customer_id
    FROM olist_orders
    WHERE order_purchase_timestamp >= '2017-01-01'
      AND order_purchase_timestamp <  '2018-01-01'
),
customers_2018 AS (
    SELECT DISTINCT customer_id
    FROM olist_orders
    WHERE order_purchase_timestamp >= '2018-01-01'
      AND order_purchase_timestamp <  '2019-01-01'
)
SELECT customer_id
FROM customers_2017
EXCEPT
SELECT customer_id
FROM customers_2018;
```


---

### Clase 6 – Performance básica: EXPLAIN, filtros sargables, índices

**Objetivos**

- Entender de forma básica `EXPLAIN` y `EXPLAIN ANALYZE`.
- Ver por qué algunas consultas no usan índices.
- Aplicar patrones simples de optimización.

**Conceptos clave**

- Consultas “sargables”: condiciones que el índice puede aprovechar.
- Diferencias entre filtrar por rango y usar funciones sobre columnas.
- Índices simples y compuestos.

**Ejemplo comparativo**

Consultas equivalentes, pero con diferente potencial de uso de índice:

```sql
-- Menos eficiente: aplica función sobre la columna
SELECT *
FROM olist_orders
WHERE DATE(order_purchase_timestamp) = '2018-01-01';

-- Más eficiente: filtra por rango
SELECT *
FROM olist_orders
WHERE order_purchase_timestamp >= '2018-01-01'
  AND order_purchase_timestamp <  '2018-01-02';
```

Se puede acompañar con la creación de un índice:

```sql
CREATE INDEX idx_orders_purchase_ts
ON olist_orders (order_purchase_timestamp);
```


---


