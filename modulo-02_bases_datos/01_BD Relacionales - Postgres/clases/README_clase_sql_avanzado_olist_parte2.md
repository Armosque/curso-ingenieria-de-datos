# Clase: SQL avanzado con Olist (Transacciones, DW, programación en BD, calidad y DE)

Esta clase continúa el trabajo con el dataset de Olist, pero ahora enfocada en:

- Transacciones, concurrencia y bloqueos.
- SQL en bases de datos analíticas y distribuidas.
- Programación en la base de datos (UDFs, procedimientos, triggers).
- Calidad, seguridad y buenas prácticas.
- SQL aplicado a ingeniería de datos.

---

## 1. Objetivos de la sesión

Al finalizar esta clase, la persona estudiante será capaz de:

- Explicar las propiedades ACID y los niveles de aislamiento de transacciones con ejemplos sobre tablas de Olist.
- Reconocer situaciones de bloqueos y deadlocks, y describir cómo mitigarlos.
- Entender diferencias entre OLTP y data warehouses usando Olist como caso.
- Diseñar estrategias básicas de particionamiento, carga incremental y manejo de historiales (SCD tipo 2).
- Crear ejemplos sencillos de funciones definidas por el usuario (UDFs), procedimientos y triggers aplicados a Olist.
- Identificar anti–patrones clásicos de SQL y proponer buenas prácticas de estilo, seguridad y calidad.
- Ver cómo encaja SQL dentro de pipelines ETL/ELT y tareas de limpieza de datos.

---

## 2. Dataset Olist: contexto rápido

Tablas principales que usaremos a lo largo de los ejemplos (ajusta nombres a tu esquema real):

- `olist_orders`
- `olist_order_items`
- `olist_order_payments`
- `olist_customers`
- `olist_products`
- `olist_order_reviews`
- `product_category_name_translation`

---

## 3. Transacciones, concurrencia y bloqueos

### 3.1. ACID y niveles de aislamiento

Recordatorio:

- Atomicidad: todo o nada.
- Consistencia: las transacciones llevan la BD de un estado válido a otro.
- Aislamiento: las transacciones se comportan como si se ejecutaran en serie.
- Durabilidad: una vez confirmada, la transacción persiste.

Ejemplo simple de transacción al actualizar estados de pedidos y pagos de Olist:

```sql
BEGIN;

UPDATE olist_orders
SET order_status = 'canceled'
WHERE order_id = '123e4567';

UPDATE olist_order_payments
SET payment_value = 0
WHERE order_id = '123e4567';

COMMIT;
-- O bien:
-- ROLLBACK;
```

Niveles de aislamiento clásicos:

- READ UNCOMMITTED
- READ COMMITTED
- REPEATABLE READ
- SERIALIZABLE

En la práctica:

- Motores OLTP suelen usar READ COMMITTED o REPEATABLE READ.
- Data warehouses suelen ofrecer aislamiento por snapshots para lecturas largas.

### 3.2. Lecturas sucias, no repetibles, phantom reads

Se explican mediante escenarios de dos sesiones trabajando sobre Olist.

Lectura sucia (dirty read)  
Sesión A:

```sql
BEGIN;
UPDATE olist_orders
SET order_status = 'shipped'
WHERE order_id = 'ABC';
-- Aún no hace COMMIT
```

Sesión B (con nivel bajo de aislamiento) podría ver el valor `'shipped'` aunque A luego haga `ROLLBACK`.

Lectura no repetible: en la primera lectura ves un valor de `order_status` para una orden, repites la lectura en la misma transacción y el valor cambió porque otra transacción hizo COMMIT en medio.

Phantom read: cuentas cuántas órdenes hay en un rango de fechas; otra transacción inserta una orden en ese rango; repites la consulta y aparece una fila “fantasma” que antes no estaba.

### 3.3. Bloqueos, deadlocks y cómo evitarlos

Escenario de deadlock:

- Transacción T1:
  - Actualiza una fila en `olist_orders`.
  - Luego intenta actualizar una fila en `olist_order_payments`.
- Transacción T2:
  - Actualiza una fila en `olist_order_payments`.
  - Luego intenta actualizar una fila en `olist_orders`.

Si cada una toma un lock que la otra necesita, aparece un deadlock.

Buenas prácticas:

- Actualizar las tablas en un orden consistente, por ejemplo, siempre `olist_orders` y luego `olist_order_payments`.
- Mantener las transacciones cortas.
- Evitar lecturas tipo `SELECT ... FOR UPDATE` sobre muchas filas si no hace falta.

### 3.4. Estrategias para consultas largas en sistemas concurrentes

- Separar cargas de trabajo de lectura y escritura, por ejemplo, usar réplicas de solo lectura para consultas pesadas sobre Olist.
- Usar niveles de aislamiento menos estrictos para reportes cuando no se requiera precisión absoluta en tiempo real.
- Usar paginación y filtros para no escanear todo Olist de una sola vez.
- Programar reportes grandes en horarios de baja carga.

---

## 4. SQL en bases de datos analíticas y distribuidas

### 4.1. De OLTP a data warehouse con Olist

En un sistema OLTP se tendrían tablas normalizadas:

- `orders`, `order_items`, `customers`, etc.

En un DW podrías tener:

- Tabla de hechos `fact_orders_items` con columnas:
  - `order_id`, `order_item_id`, `product_id`, `customer_id`, fechas, precios, freight, etc.
- Tablas de dimensiones:
  - `dim_customer`, `dim_product`, `dim_date`.

Ejemplo de consulta analítica:

```sql
SELECT
    d.year,
    p.product_category_name,
    SUM(f.price + f.freight_value) AS revenue
FROM fact_orders_items f
JOIN dim_date d
    ON f.order_purchase_date_key = d.date_key
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY d.year, p.product_category_name;
```

### 4.2. Particionamiento de tablas

Ejemplo: particionar una tabla de hechos de Olist por fecha de compra (sintaxis aproximada para Postgres 11+):

```sql
CREATE TABLE fact_orders_items (
    order_id                TEXT,
    order_item_id           INT,
    order_purchase_date     DATE,
    customer_id             TEXT,
    product_id              TEXT,
    price                   NUMERIC(10,2),
    freight_value           NUMERIC(10,2)
) PARTITION BY RANGE (order_purchase_date);
```

Luego particiones por año:

```sql
CREATE TABLE fact_orders_items_2017
    PARTITION OF fact_orders_items
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
```

Beneficios:

- Consultas filtradas por fecha escanean menos datos.
- Mantenimiento más sencillo (particiones viejas se archivan o eliminan).

### 4.3. Sharding lógico vs físico (conceptual)

- Sharding lógico: dividir los datos por alguna clave de negocio, por ejemplo, por región de `customer_state`, y enviarlos a distintas instancias.
- Sharding físico: cómo el proveedor distribuye internamente tus datos entre nodos, a menudo transparente para ti.

Ejemplo conceptual con Olist:

- Clientes de ciertos estados en un shard.
- Clientes del resto en otro shard.

A nivel SQL, intentas que las consultas respeten esa distribución para evitar joins entre shards.

### 4.4. Diferencias SQL en data warehouses frente a OLTP

En muchos DW:

- Menos restricciones de claves foráneas.
- Esquemas tipo estrella o copo de nieve.
- Orientados a agregaciones grandes y funciones de ventana.
- SQL extendido con funciones analíticas, `GROUPING SETS`, `ROLLUP`, `CUBE`.

Con Olist en un DW, es común escribir consultas como:

```sql
SELECT
    customer_state,
    DATE_TRUNC('month', order_purchase_timestamp) AS month,
    SUM(price + freight_value) AS revenue
FROM fact_orders_items
GROUP BY customer_state, DATE_TRUNC('month', order_purchase_timestamp);
```

### 4.5. Carga incremental y SCD tipo 2

Caso clásico: historial de datos de clientes.

Tabla de dimensión tipo 2 para Olist:

```sql
CREATE TABLE dim_customer (
    customer_sk             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id             TEXT,
    customer_unique_id      TEXT,
    customer_city           TEXT,
    customer_state          TEXT,
    valid_from              DATE,
    valid_to                DATE,
    is_current              BOOLEAN
);
```

Lógica simplificada de SCD 2:

- Cuando cambian `customer_city` o `customer_state` para un `customer_id`:
  - Cerrar el registro actual (`valid_to` y `is_current = FALSE`).
  - Insertar un nuevo registro con `valid_from = fecha_cambio`, `valid_to = '9999-12-31'`, `is_current = TRUE`.

---

## 5. Programación en la base de datos

### 5.1. Funciones definidas por el usuario (UDFs)

Ejemplo: clasificar reseñas de Olist según `review_score`.

```sql
CREATE OR REPLACE FUNCTION classify_review(score INT)
RETURNS TEXT AS $$
BEGIN
    IF score <= 2 THEN
        RETURN 'mala';
    ELSIF score = 3 THEN
        RETURN 'neutral';
    ELSE
        RETURN 'buena';
    END IF;
END;
$$ LANGUAGE plpgsql;
```

Uso:

```sql
SELECT
    review_id,
    review_score,
    classify_review(review_score) AS review_label
FROM olist_order_reviews;
```

### 5.2. Procedimientos almacenados

Ejemplo simplificado: recalcular una tabla de resumen de revenue diario.

```sql
CREATE OR REPLACE PROCEDURE refresh_daily_revenue()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE agg_daily_revenue;

    INSERT INTO agg_daily_revenue (order_date, revenue)
    SELECT
        DATE(o.order_purchase_timestamp) AS order_date,
        SUM(oi.price + oi.freight_value) AS revenue
    FROM olist_orders o
    JOIN olist_order_items oi
        ON o.order_id = oi.order_id
    GROUP BY DATE(o.order_purchase_timestamp);
END;
$$;
```

### 5.3. Triggers

Ejemplo conceptual: actualizar una tabla con conteo de reseñas cuando entra una nueva reseña.

```sql
CREATE TABLE agg_review_score_by_order (
    order_id        TEXT PRIMARY KEY,
    num_reviews     INT,
    avg_score       NUMERIC(3,2)
);
```

Trigger:

```sql
CREATE OR REPLACE FUNCTION update_review_agg()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO agg_review_score_by_order (order_id, num_reviews, avg_score)
    SELECT
        NEW.order_id,
        COUNT(*),
        AVG(review_score)
    FROM olist_order_reviews
    WHERE order_id = NEW.order_id
    ON CONFLICT (order_id) DO UPDATE
    SET
        num_reviews = EXCLUDED.num_reviews,
        avg_score = EXCLUDED.avg_score;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_review_insert
AFTER INSERT ON olist_order_reviews
FOR EACH ROW
EXECUTE FUNCTION update_review_agg();
```

### 5.4. Pros y contras de meter lógica en la BD

Pros:

- Cercanía a los datos.
- Consistencia centralizada.
- Menos ida y vuelta entre aplicación y base de datos.

Contras:

- Lógica más difícil de versionar y testear que el código de aplicación.
- Acoplamiento fuerte a un motor específico.
- Riesgos de sobrecargar la BD con lógica compleja.

---

## 6. Calidad, seguridad y buenas prácticas

### 6.1. Normas de estilo en SQL

Recomendaciones:

- Nombres descriptivos y consistentes (snake_case).
- Palabras reservadas en mayúsculas, identificadores en minúsculas.
- Formato con sangrías claras, un JOIN por línea.

Ejemplo legible:

```sql
SELECT
    c.customer_state,
    DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
    SUM(oi.price + oi.freight_value) AS revenue
FROM olist_orders AS o
JOIN olist_customers AS c
    ON o.customer_id = c.customer_id
JOIN olist_order_items AS oi
    ON o.order_id = oi.order_id
GROUP BY c.customer_state, DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY order_month, c.customer_state;
```

### 6.2. Anti–patrones clásicos

- Spaghetti query: una sola query gigante, sin CTEs ni estructura.
- SELECT * en producción: problemas de rendimiento, acoplamiento al esquema.
- Funciones en columnas indexadas sin cuidado:
  
  ```sql
  WHERE DATE(order_purchase_timestamp) = '2017-01-01'
  ```
  
  Mejor:
  
  ```sql
  WHERE order_purchase_timestamp >= '2017-01-01'
    AND order_purchase_timestamp < '2017-01-02'
  ```

- Subconsultas innecesarias que podrían ser joins más simples.

### 6.3. SQL seguro: inyección y permisos

Ejemplo de consulta vulnerable (pseudo código en Python):

```python
order_id = input("Order id?: ")
query = f"SELECT * FROM olist_orders WHERE order_id = '{order_id}'"
```

Ejemplo parametrizado:

```python
cursor.execute(
    "SELECT * FROM olist_orders WHERE order_id = %s",
    (order_id,)
)
```

Control de permisos:

```sql
CREATE ROLE reporting_user;
GRANT CONNECT ON DATABASE olist_db TO reporting_user;
GRANT USAGE ON SCHEMA public TO reporting_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reporting_user;
```

### 6.4. Testing básico de SQL

Ejemplos de checks de calidad con Olist:

Conteos de filas:

```sql
SELECT COUNT(*) FROM olist_orders;
SELECT COUNT(*) FROM olist_order_items;
```

Chequear referencialidad:

```sql
SELECT COUNT(*)
FROM olist_order_items oi
LEFT JOIN olist_orders o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;
```

Validar rangos de valores:

```sql
SELECT MIN(price), MAX(price)
FROM olist_order_items;
```

---

## 7. SQL para ingeniería de datos

### 7.1. Uso de SQL en pipelines ETL/ELT

Patrón típico con Olist:

1. Ingesta en tablas raw (`raw_olist_orders`, etc.).
2. Limpieza y normalización en tablas staging.
3. Carga en tablas modeladas (hechos y dimensiones).

Ejemplo de ELT (staging a fact) con CTAS:

```sql
CREATE TABLE fact_orders_items AS
SELECT
    o.order_id,
    o.customer_id,
    DATE(o.order_purchase_timestamp) AS order_date,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value
FROM stg_olist_orders o
JOIN stg_olist_order_items oi
    ON o.order_id = oi.order_id;
```

### 7.2. Limpieza de datos con SQL

Tratamiento de nulos y categorías desconocidas:

```sql
SELECT
    COALESCE(payment_type, 'UNKNOWN') AS payment_type_label,
    COUNT(*) AS num_orders
FROM olist_order_payments
GROUP BY COALESCE(payment_type, 'UNKNOWN');
```

Normalización de texto:

```sql
SELECT
    LOWER(TRIM(product_category_name)) AS product_category_clean,
    COUNT(*) AS num_products
FROM olist_products
GROUP BY LOWER(TRIM(product_category_name));
```

Outliers básicos:

```sql
SELECT
    percentile_disc(0.99) WITHIN GROUP (ORDER BY price) AS p99_price
FROM olist_order_items;
```

Luego usar ese percentil para revisar valores extremos.

---
