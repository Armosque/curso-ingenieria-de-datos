# Guía de Laboratorio: AWS Serverless Data Lake Jumpstart



**Enlace al Taller Oficial:** [AWS Serverless Data Lake Jumpstart](https://catalog.us-east-1.prod.workshops.aws/workshops/276faf92-bffc-4843-8a8e-8078add48194/en-US)

---

## Fase 1: Despliegue de Infraestructura (EN CLASE)

**⚠️ IMPORTANTE:** No inicies esta fase por tu cuenta. Espera las indicaciones  para lanzar el CloudFormation.

El objetivo es crear la infraestructura base (Buckets S3, Roles IAM, VPCs) necesaria para los laboratorios.

### Instrucción Clave: Personalización
Para asegurar que tengas tu propio entorno de trabajo dentro de la cuenta compartida, debes seguir este paso estrictamente:

1.  Al lanzar el **Stack de CloudFormation** (siguiendome).
2.  En la pantalla **"Specify stack details"** (Paso 2).
3.  Ubica el campo **Stack name** (Nombre de la pila) o los **Parámetros** definidos para nombrar recursos (ej. `BucketPrefix`).
4.  **AGREGA TUS INICIALES** al final del nombre predeterminado.
    * *Ejemplo:* `ServerlessDataLake-JEM` (Si te llamas Juan Esteban Mejía).

> **Verificación:** Asegúrate de que el Stack llegue al estado `CREATE_COMPLETE` antes de continuar.

---

## Fase 2: Ejecución de Laboratorios (INDIVIDUAL)

Una vez creada la infraestructura en clase, debes completar los siguientes módulos del taller oficial de manera autónoma.

### 1. Ingesta y Almacenamiento (Ingestion & Storage)
* **Objetivo:** Subir la data cruda a S3.
* **Cuidado:** Asegúrate de subir los archivos al Bucket S3 que tiene **tus iniciales** (creado en la Fase 1). No uses buckets genéricos ni de otros compañeros.

### 2. Catalogación y Transformación (Cataloging & ETL)
* **Objetivo:** Usar AWS Glue para descubrir el esquema y transformar datos.
* **Configuración:** Cuando configures el **Crawler** y los **Jobs de Glue**, apunta siempre a las rutas S3 de tu bucket personalizado (`s3://...-JPR/`).

### 3. Analítica y Visualización (Analytics & Visualization)
* **Objetivo:** Consultar datos con Athena y visualizar con QuickSight.
* **Actividad:** Ejecuta consultas SQL sobre tus tablas y genera un gráfico simple.

---

## Fase 3: Bitácora de Discusión (TAREA)

Para la próxima clase, debes traer preparada la siguiente información basada en tu experiencia durante la Fase 2. Se abrirá un espacio de discusión.

### 1. Dudas y Bloqueos (Mínimo 2)
Registra preguntas sobre errores que te aparecieron o conceptos que no quedaron claros.
* *Ejemplo:* "¿Por qué necesito un Crawler si ya sé qué columnas tiene mi CSV?"
* *Ejemplo:* "Me salió el error X en Athena al consultar..."

### 2. Aportes o Hallazgos (Mínimo 1)
Registra algo nuevo que hayas aprendido, una configuración interesante o una optimización.
* *Ejemplo:* "Descubrí que guardar en formato Parquet reduce el costo de escaneo en Athena."

### 3. Evidencia de Ejecución
* **Captura de Pantalla:** Debes presentar una imagen de la consola de **CloudFormation** donde se vea claramente el **Nombre del Stack con tus iniciales** en estado `CREATE_COMPLETE`.

---

## ⚠️ Limpieza de Recursos
* Recuerda vaciar los Buckets S3 antes de intentar borrar el Stack de CloudFormation.
* Elimina cualquier recurso creado manualmente (Endpoints de Glue) para evitar costos extra.