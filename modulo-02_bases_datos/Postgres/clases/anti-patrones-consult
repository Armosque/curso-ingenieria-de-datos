# Antipatrón SQL: 


### *Fear of the Unknown* (miedo a los `NULL`)  

## 1. Idea central del antipatrón


> En lugar de usar `NULL` para representar valores desconocidos o no aplicables, los desarrolladores inventan “valores mágicos” (0, `-1`, `'N/A'`, `'1900-01-01'`, etc.) porque *no quieren* tratar con `NULL`.

Problemas que esto genera:

- Distorsiona los cálculos (promedios, conteos, etc.).
- Rompe la integridad referencial (claves foráneas “falsas”).
- Obliga a recordar convenciones ocultas (“en esta columna `-1` significa ‘desconocido’”).
- Nuevos miembros del equipo no entienden la semántica real de los datos.

Recomendación:

- **Usa `NULL` cuando el valor es realmente desconocido o no aplica.**
- Trátalo de forma explícita (`IS NULL`, `IS NOT NULL`, `COALESCE`…) en consultas y reportes.
- Evita “sobrecargar” valores numéricos o strings para significar “desconocido”.

---

## 2. Contexto: tablas relevantes de Olist

En el dataset de Olist usaremos principalmente:

- `olist_orders`
  - `order_delivered_customer_date` (fecha de entrega al cliente).
  - `order_approved_at`, `order_delivered_carrier_date`, etc.
- `olist_order_reviews`
  - `review_comment_message` (comentario de texto).
- `olist_order_payments`
  - `payment_type` (tipo de pago).

En estos campos es perfectamente normal que existan `NULL`:

- Pedidos no entregados, fecha de entrega desconocida.
- Reviews sin comentario, mensaje de texto inexistente.
- Registros de pago incompletos, tipo de pago faltante.

---

## 3. Ejemplo 1: fecha de entrega de pedidos

### 3.1. Modelo “sano” (usa `NULL`)

En `olist_orders`:

```sql
order_delivered_customer_date TIMESTAMP NULL
