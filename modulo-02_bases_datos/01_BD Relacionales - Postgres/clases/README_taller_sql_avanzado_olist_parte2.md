# Taller: SQL avanzado con Olist (Transacciones, DW, programación en BD, calidad y DE)

Este taller complementa la clase sobre:

- Transacciones, concurrencia y bloqueos.
- SQL en bases analíticas y distribuidas.
- Programación en la base de datos.
- Calidad, seguridad y buenas prácticas.
- SQL para ingeniería de datos.

Trabaja en parejas o grupos pequeños. No hay una única solución para varias tareas, lo importante es la discusión.

> Sugerencia docente: seleccionar un subconjunto de ejercicios según el tiempo disponible.

---

## 1. Transacciones, concurrencia y bloqueos

### Ejercicio 1 – Transacción consistente

Diseña una transacción que:

1. Marque un pedido en `olist_orders` como `canceled`.
2. Ajuste sus pagos en `olist_order_payments` para tener `payment_value = 0`.
3. Inserte un registro de auditoría en una tabla ficticia `order_cancellations_audit` con:
   - `order_id`
   - `cancel_reason`
   - `cancel_timestamp`.

Requisitos:

- Usa `BEGIN`, `COMMIT`, `ROLLBACK`.
- Explica qué pasaría si ocurre un error después de actualizar `olist_orders` pero antes de insertar en la tabla de auditoría.

---

### Ejercicio 2 – Escenario de lectura no repetible

Imagina dos sesiones (A y B) trabajando sobre `olist_orders`:

- Sesión A quiere leer el estado de un pedido al comienzo de una transacción y más adelante volver a leerlo.
- Sesión B actualiza el `order_status` de ese pedido y hace `COMMIT` en medio.

Tareas:

1. Escribe la secuencia de comandos SQL de ambas sesiones que produzca una lectura no repetible.
2. Explica qué nivel de aislamiento permitiría este comportamiento y qué nivel lo evitaría.

---

### Ejercicio 3 – Deadlock conceptual

Define un escenario de deadlock entre dos transacciones usando tablas:

- `olist_orders`
- `olist_order_payments`

Tareas:

1. Escribe los pasos de las transacciones T1 y T2 que llevan a un deadlock.
2. Propón una regla de orden de actualización de tablas que evite este deadlock.

---

### Ejercicio 4 – Consultas largas y concurrencia

Supón que alguien ejecuta una consulta pesada de agregación sobre `olist_order_items` y `olist_orders` para calcular revenue histórico por día.

1. ¿Qué problemas puede causar en un ambiente OLTP con muchas escrituras concurrentes?
2. Propón al menos dos estrategias para mitigar estos problemas (desde configuración de aislamiento hasta separación de cargas o uso de réplicas).

---

## 2. SQL en bases de datos analíticas y distribuidas

### Ejercicio 5 – De Olist OLTP a fact table

A partir de `olist_orders` y `olist_order_items`:

1. Diseña el esquema de una tabla de hechos `fact_orders_items` para un data warehouse.
2. Indica:
   - Clave(s) principal(es).
   - Claves foráneas lógicas hacia dimensiones (aunque no las crees).
   - Campos de medidas.

Opcional: escribe un `CREATE TABLE` aproximado.

---

### Ejercicio 6 – Particionamiento por fecha

Diseña una estrategia de particionamiento para `fact_orders_items` basada en la fecha de compra.

1. ¿Particionarías por día, mes o año? Justifica.
2. Escribe un ejemplo de `CREATE TABLE` con particionamiento por rango sobre la columna `order_purchase_date`.
3. Explica cómo afectaría a una consulta que filtra un rango de fechas de un año específico.

---

### Ejercicio 7 – Sharding lógico

Imagina que vas a hacer sharding lógico de clientes de Olist en dos clústeres:

- Shard 1: ciertos estados.
- Shard 2: el resto.

Tareas:

1. Propón una regla de asignación de shards basada en `customer_state`.
2. Describe qué problemas podrían surgir cuando necesitas hacer un análisis que cruce datos de ambos shards.
3. Sugiere una estrategia para estos análisis inter-shard.

---

### Ejercicio 8 – Carga incremental y SCD2

Supón que tienes una dimensión de clientes `dim_customer` tipo 2 (historial) y una tabla de staging `stg_olist_customers` con la foto más reciente de los clientes.

1. Diseña las columnas clave de `dim_customer` (incluyendo fechas y `is_current`).
2. Escribe, en pseudocódigo SQL o MERGE, el flujo para:
   - Detectar cambios de ciudad o estado.
   - Cerrar el registro antiguo y crear uno nuevo cuando hay cambios.

No es necesario que el SQL sea perfecto; lo importante es capturar la lógica.

---

## 3. Programación en la base de datos

### Ejercicio 9 – UDF de clasificación de reseñas

Crea una función definida por el usuario que reciba `review_score` y devuelva:

- `mala` si score es 1 o 2.
- `neutral` si score es 3.
- `buena` si score es 4 o 5.

1. Escribe la función.
2. Escribe una consulta sobre `olist_order_reviews` que use esta función y cuente cuántas reseñas hay de cada tipo por `customer_state` (usando join con `olist_orders` y `olist_customers`).

---

### Ejercicio 10 – Procedimiento para tabla de agregados

Define un procedimiento almacenado `refresh_daily_revenue` que:

1. Vacíe o reemplace el contenido de una tabla `agg_daily_revenue`.
2. Inserte en ella el revenue diario (suma de `price + freight_value`) por fecha de compra.

Tareas:

- Escribe el cuerpo del procedimiento de manera aproximada.
- Indica cuándo y cómo lo ejecutarías en un ambiente real (por ejemplo, diarias, usando un scheduler).

---

### Ejercicio 11 – Trigger de auditoría de reseñas

Crea el diseño de:

1. Una tabla de auditoría `review_audit` con:
   - `review_id`
   - `order_id`
   - `old_score`
   - `new_score`
   - `changed_at`
2. Un trigger que se dispare cada vez que se actualice `review_score` en `olist_order_reviews` y registre el cambio en `review_audit`.

No es necesario que el SQL compile; el objetivo es practicar la estructura.

---

## 4. Calidad, seguridad y buenas prácticas

### Ejercicio 12 – Refactor de spaghetti query

Tienes una consulta imaginaria que:

- Une `olist_orders`, `olist_customers`, `olist_order_items`, `olist_order_reviews`, `olist_products`.
- Calcula muchas métricas en un solo SELECT, sin CTEs ni comentarios.

Tareas:

1. Esboza esa consulta en una sola pieza.
2. Refactórala en al menos dos CTEs, por ejemplo:
   - `order_totals` para totales por orden.
   - `order_reviews_agg` para agregados de reseñas.
3. Comparte la versión refactorizada, más legible.

---

### Ejercicio 13 – Anti–patrones en Olist

Analiza y corrige las siguientes prácticas (escribe alternativas mejores):

1. `SELECT * FROM olist_orders;` usado en un reporte en producción.
2. Filtro de fecha:

   ```sql
   SELECT *
   FROM olist_orders
   WHERE DATE(order_purchase_timestamp) = '2017-01-01';
   ```

3. Subconsulta anidada innecesaria para calcular revenue total cuando se podría usar un join simple con agregación.

---

### Ejercicio 14 – SQL seguro y permisos

1. Escribe un ejemplo de consulta construida dinámicamente que podría ser vulnerable a inyección SQL si se concatenan valores de entrada del usuario.
2. Reescribe el ejemplo usando parámetros.
3. Diseña una política básica de permisos para un rol `reporting_user` que solo pueda:
   - Conectarse a la base.
   - Leer tablas de Olist.
   - No pueda modificar datos.

---

### Ejercicio 15 – Checks de calidad con Olist

Define al menos tres consultas de prueba para verificar la calidad de los datos en Olist, por ejemplo:

- Chequear que todos los `order_id` de `olist_order_items` existan en `olist_orders`.
- Chequear que no haya precios negativos.
- Chequear que las fechas de entrega estimada sean posteriores a las fechas de compra.

Escribe las consultas SQL correspondientes.

---

## 5. SQL para ingeniería de datos

### Ejercicio 16 – Pipeline ELT simple

Imagina que recibes datos nuevos de Olist diariamente en tablas `raw_olist_*`:

1. Diseña un pipeline en tres capas:
   - `raw_` (ingesta).
   - `stg_` (staging con limpieza mínima).
   - `dim_` y `fact_` (modelo final).
2. Escribe al menos:
   - Una sentencia `INSERT INTO ... SELECT` de `raw_` a `stg_`.
   - Una sentencia `CREATE TABLE AS SELECT` o `INSERT` de `stg_` a `fact_`.

---

### Ejercicio 17 – Limpieza de nulos y categorías

Para la tabla `olist_order_payments`:

1. Propón una regla de negocio para manejar `payment_type` nulo o desconocido.
2. Escribe una consulta que:
   - Trate nulos de `payment_type` como `UNKNOWN` solo en la capa de reporte (sin sobrescribir la tabla).
   - Cuente el número de órdenes por `payment_type_label`.

---

### Ejercicio 18 – Outliers y valores extremos

Con `olist_order_items`:

1. Calcula el percentil 99 de `price`.
2. Identifica cuántos registros tienen `price` por encima de ese percentil.
3. Discute:
   - En qué contextos considerarías estos registros como outliers problemáticos.
   - Qué harías con ellos en un pipeline de ingeniería de datos (eliminar, marcar, capear, etc.).

---

## 6. Recomendaciones finales

- Documenta tus consultas añadiendo comentarios breves en el SQL.
- Guarda las soluciones en archivos `.sql` o notebooks, versionados con Git.
- En una segunda sesión, puedes:
  - Medir tiempos de ejecución de algunas consultas.
  - Experimentar con índices o particiones.
  - Comparar planes de ejecución entre versiones refactorizadas.

---
