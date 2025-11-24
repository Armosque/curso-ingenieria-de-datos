# Taller: SQL avanzado con el dataset de Olist

Este taller está diseñado para practicar los temas vistos en la clase de SQL avanzado:

- Repaso intermedio (JOINs, agregaciones, subconsultas).
- Consultas avanzadas (subconsultas correlacionadas, CTEs, anti/semi-joins, pivot).
- Funciones de ventana.
- SQL analítico y de reporting.
- Rendimiento y optimización básica.

La idea es que trabajes en parejas o grupos pequeños. Las preguntas están organizadas por bloques; puedes saltar entre bloques según el ritmo de la clase.

> Nota: no se incluyen soluciones en este archivo. Se recomienda tener un archivo separado con soluciones para uso del docente.

---

## 1. Bloque A – Repaso intermedio

### A1. JOINs

1. Lista las primeras 20 órdenes incluyendo:
   - `order_id`
   - `order_purchase_timestamp`
   - `customer_city`
   - `customer_state`
   - número de ítems de la orden

   Usa `olist_orders`, `olist_customers` y `olist_order_items`.

2. Encuentra los clientes que han comprado a más de **un seller distinto**. Muestra:
   - `customer_unique_id`
   - número de sellers distintos (`num_sellers_distintos`)

   Pista: agrupa por `customer_unique_id` y cuenta `DISTINCT seller_id`.

---

### A2. Agregaciones

3. Calcula el **ticket promedio** (precio + flete) por estado (`customer_state`). Muestra:
   - `customer_state`
   - número de órdenes
   - ticket promedio

   Ordena el resultado de mayor a menor ticket promedio.

4. Para cada `product_category_name`, calcula:
   - número de órdenes
   - revenue total (suma de `price`)
   - revenue promedio por orden

   Usa `olist_order_items`, `olist_products` y `olist_orders`.

---

### A3. Subconsultas simples

5. Calcula cuántos pedidos tiene cada `customer_unique_id`. Luego, usando una subconsultas, devuelve solo los clientes que tienen **más pedidos que el promedio** de pedidos por cliente.

6. Crea una subconsulta que calcule el **revenue promedio por categoría de producto** y luego, en la consulta externa, devuelve solo las categorías cuyo revenue sea **mayor al promedio global**.

---

## 2. Bloque B – Consultas avanzadas

### B1. Subconsultas correlacionadas

7. Para cada pedido, calcula el total del pedido (`price + freight_value`) y filtra solo aquellos cuya suma sea **mayor que el promedio de flete de su estado**. Usa una subconsulta correlacionada que compare contra el promedio de flete del mismo `customer_state`.

8. Encuentra los pedidos cuyo valor de flete sea **mayor que el máximo flete** pagado por ese mismo cliente en cualquier otro pedido. Usa una subconsulta correlacionada sobre `customer_id`.

---

### B2. CTEs

9. Crea un CTE llamado `order_totals` con las columnas:
   - `order_id`
   - `customer_unique_id`
   - `order_purchase_timestamp`
   - `order_total` (suma de `price + freight_value`)

   A partir de ese CTE, calcula el **ticket promedio por cliente** y devuelve los 10 clientes con mayor ticket promedio.

10. Crea un CTE recursivo que genere una tabla de fechas entre la fecha mínima y la fecha máxima de `order_purchase_timestamp` de `olist_orders`. Luego, haz un LEFT JOIN con las órdenes para contar el número de pedidos por día, incluyendo días sin pedidos (que deben aparecer con 0).

---

### B3. Anti-joins y semi-joins

11. Lista los `customer_unique_id` que **nunca han recibido una reseña**. Usa `NOT EXISTS` con `olist_order_reviews`.

12. Lista los `customer_unique_id` que **al menos una vez** han recibido una reseña con `review_score = 5`. Usa `EXISTS`.

13. Calcula, para cada cliente, el número de pedidos que tienen reseña y el número de pedidos que no tienen reseña. Identifica a los clientes que tienen **al menos un pedido sin reseña**.

---

### B4. Pivot básico

14. Construye una tabla que muestre, por `customer_state`, cuántas reseñas de cada puntuación (`1` a `5`) hay. El resultado debe tener columnas:

- `customer_state`
- `score_1`
- `score_2`
- `score_3`
- `score_4`
- `score_5`

Puedes usar:

- `COUNT(*) FILTER (WHERE review_score = N)`  
- o `SUM(CASE WHEN review_score = N THEN 1 ELSE 0 END)`.

---

## 3. Bloque C – Funciones de ventana

### C1. Clasificación con `ROW_NUMBER`, `RANK`, `DENSE_RANK`

15. Para cada `customer_unique_id`, numera sus pedidos por fecha de compra usando `ROW_NUMBER`. Devuelve solo la **primera compra** de cada cliente.

16. Calcula, para cada producto, su revenue total y su posición de ranking dentro de su categoría (`product_category_name`), usando `RANK` o `DENSE_RANK`. Muestra:

- `product_id`
- `product_category_name`
- `revenue_total`
- `rank_en_categoria`

---

### C2. Acumulados y ventanas móviles

17. Calcula el revenue por mes (suma de `price + freight_value` agrupado por mes de `order_purchase_timestamp`). Luego añade:

- revenue mensual
- revenue acumulado global (desde el primer mes)
- revenue de los últimos 3 meses (ventana móvil)

Pista: puedes usar `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.

18. Para cada estado (`customer_state`), calcula el revenue mensual y el revenue acumulado solo dentro de ese estado. Ordena por estado y por mes.

---

### C3. LAG y variaciones

19. Para cada cliente, calcula la diferencia en días entre cada compra y la anterior usando `LAG(order_purchase_timestamp)`. Luego, crea una columna categórica:

- `frecuente` si el intervalo es menor o igual a 30 días
- `ocasional` si el intervalo es entre 31 y 90 días
- `raro` si el intervalo es mayor a 90 días

20. Calcula el revenue mensual global y la variación porcentual mes a mes:

- `revenue`
- `prev_revenue` (con `LAG`)
- `variacion_pct` igual a `100 * (revenue - prev_revenue) / prev_revenue`

---

## 4. Bloque D – SQL analítico y reporting

### D1. GROUPING SETS / ROLLUP

21. Construye una consulta que devuelva:

- revenue por año
- revenue por año y mes
- revenue total

Usa `GROUPING SETS`. Añade una columna que etiquete el nivel de agregación, por ejemplo:

- `nivel = 'TOTAL'`
- `nivel = 'AÑO'`
- `nivel = 'AÑO_MES'`

22. Usando `ROLLUP`, calcula el revenue por año y por categoría de producto (`product_category_name`), incluyendo:

- totales por año
- gran total

---

### D2. Tablas derivadas y data marts lógicos

23. A partir de una tabla derivada `order_mart` que tenga:

- `order_id`
- `customer_state`
- `order_total`
- `order_date` (solo fecha, sin hora)

Construye un reporte que muestre:

- revenue diario por estado
- top 3 estados por revenue total

Puedes implementar `order_mart` como una subconsulta en el `FROM`.

---

## 5. Bloque E – Rendimiento y optimización

### E1. Planes de ejecución

24. Elige una consulta del taller que use una subconsulta correlacionada y reescríbela usando joins y `GROUP BY`. Ejecuta `EXPLAIN` (o `EXPLAIN ANALYZE`) para ambas versiones y responde:

- ¿Cuál de las dos versiones procesa más filas?
- ¿Qué tipo de join aparece (por ejemplo, Nested Loop, Hash Join)?
- ¿Cuál crees que escalará mejor cuando los datos crezcan?

---

### E2. Paginación

25. Construye una consulta que liste pedidos ordenados por `order_purchase_timestamp` y pagine los resultados usando `LIMIT` y `OFFSET` (por ejemplo, página 1, página 2, página 3).

Luego, plantea una alternativa para paginación basada en cursores usando una condición del tipo:

```sql
WHERE (order_purchase_timestamp, order_id) > (...)
```

Preguntas para discusión:

- ¿Qué desventajas tiene `OFFSET` en tablas muy grandes?
- ¿Por qué la paginación por cursor puede ser más eficiente y consistente?



---
