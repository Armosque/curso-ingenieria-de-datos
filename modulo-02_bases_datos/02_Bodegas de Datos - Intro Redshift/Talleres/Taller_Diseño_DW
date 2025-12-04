# Ejercicios Operaciones OLAP

## I. Operaciones OLAP

### Durante Clase

**1. Un almacén de datos de un proveedor de telefonía.**
Consta de cinco dimensiones: *cliente que llama, cliente que recibe la llamada, fecha, tipo de llamada* y *programa de llamada*; y tres medidas: *número de llamadas, duración* e *importe*.
Defina las operaciones OLAP que se realizarán para responder a las siguientes consultas. Proponga jerarquías de dimensiones cuando sea necesario.

   a. Importe total recaudado por cada programa de llamadas en 2012.
   b. Duración total de las llamadas realizadas por clientes desde Bruselas en 2012.
   c. Número total de llamadas de fin de semana realizadas por clientes desde Bruselas a clientes en Amberes en 2012.
   d. Duración total de las llamadas internacionales iniciadas por clientes en Bélgica en 2012.
   e. Importe total recaudado de los clientes en Bruselas inscritos en el programa corporativo en 2012.

**2. Un almacén de datos de una compañía ferroviaria.**
Contiene información sobre los segmentos de tren. Consta de seis dimensiones: *estación de salida, estación de llegada, viaje, tren, hora de llegada* y *hora de salida*; y tres medidas: *número de pasajeros, duración* y *kilómetros*.
Defina las operaciones OLAP que se realizarán para responder a las siguientes consultas. Proponga jerarquías de dimensiones cuando sea necesario.

   a. Número total de kilómetros recorridos por los trenes de Alstom durante 2012 con salida desde estaciones francesas o belgas.
   b. Duración total de los viajes internacionales durante 2012 (viajes con salida desde una estación ubicada en un país y llegada a una estación ubicada en otro país).
   c. Número total de viajes con salida o llegada a París durante julio de 2012.
   d. Duración media de los segmentos de tren en Bélgica en 2012.
   e. Para cada viaje, número promedio de pasajeros por segmento (tomar todos los segmentos de cada viaje y promediar el número de pasajeros).

### Hacer de manera individual

**3. Considere el almacén de datos de una universidad.**
Contiene información sobre actividades docentes e investigadoras.
* **Actividades docentes:** Se relacionan con las dimensiones *departamento, profesor, curso* y *tiempo* (granularidad: semestre académico). Las medidas son: *número de horas* y *número de créditos*.
* **Actividades de investigación:** Se relacionan con las dimensiones *profesor, organismo financiador, proyecto* y *tiempo* (dos veces: fecha inicio y fin, granularidad: día). Los profesores se relacionan con el departamento al que están afiliados. Las medidas son: *número de meses-persona* e *importe*.

Defina las operaciones OLAP que se deben realizar para responder a las siguientes consultas. Para ello, proponga las jerarquías de dimensiones necesarias.

   a. Por departamento, número total de horas lectivas durante el año académico 2012-2013.
   b. Por departamento, número total de proyectos de investigación durante el año calendario 2012.
   c. Por departamento, número total de profesores que participaron en proyectos de investigación durante el año calendario 2012.
   d. Por profesor, número total de cursos impartidos durante el año académico 2012-2013.
   e. Por departamento y organismo financiador, número total de proyectos iniciados en 2012.

---

## II. Ejercicios Modelo Conceptual

### Durante Clase
2.1 Diseñe un esquema MultiDim para la aplicación del proveedor de telefonía del **ejemplo 1**.
2.2 Diseñe un esquema MultiDim para la aplicación de trenes del **ejemplo 2**.

### Hacer de manera individual
2.3 Diseñe un esquema MultiDim para la aplicación universitaria del **ejemplo 3**, considerando las diferentes granularidades de la dimensión temporal.

---

## III. Ejercicios Modelo Lógico

### Durante Clase
3.1. Considere el almacén de datos del proveedor de telefonía del **ejemplo 2.1**. Dibuje un diagrama de estrella para el almacén de datos.
3.2 Considere el almacén de datos de la aplicación de trenes del **ejemplo 2.2**. Dibuje un diagrama de copo de nieve para el almacén de datos con jerarquías para las dimensiones de tren y estación.

### Hacer de manera individual
3.3 Considere el almacén de datos universitario descrito en el **ejemplo 2.3**. Dibuje un esquema de constelación para el almacén de datos teniendo en cuenta las diferentes granularidades de la dimensión temporal.