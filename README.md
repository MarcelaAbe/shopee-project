# Recolección de Tiendas Oficiales de Shopee por Categoría

Este proyecto automatiza la recolección de datos de **tiendas oficiales** de Shopee, segmentadas por categoría, y su posterior procesamiento para normalización y carga en BigQuery.

---

## 🤖 Resumen General

El proceso consta de dos etapas principales:

1. **Crawler en Node.js**  
   Se realiza la consulta a la API pública de Shopee por categoría para obtener las tiendas oficiales. Los datos se recolectan en formato JSON y se exportan a CSV con timestamp.  

2. **Tratamiento y Normalización en Python**  
   El CSV generado es cargado en un DataFrame pandas para limpieza de nombres, normalización de cadenas, generación de IDs únicos (`UNIC_ID`) para marcas nuevas, verificación y actualización con datos históricos en BigQuery, y finalmente carga en una tabla BigQuery para análisis posteriores.

---

## 📦 Funcionalidades

### Crawler (Node.js)

- Solicita datos a la API pública de Shopee por categoría  
- Usa headers simulando navegador real para evitar bloqueos  
- Recupera categorías agrupadas y no agrupadas (tiendas nuevas)  
- Exporta CSV con timestamp  
- Preparado para integración con BigQuery o bases externas

### Post-Procesamiento (Python)

- Normaliza nombres de usuario y marca (mayúsculas, elimina símbolos y tildes)  
- Detecta y reporta caracteres especiales no deseados  
- Genera `UNIC_ID` alfanumérico único para tiendas nuevas  
- Consulta tabla histórica en BigQuery para reutilizar IDs existentes  
- Agrega columnas de auditoría de fechas  
- Carga y actualiza tabla BigQuery con datos limpios y normalizados

---

## 🧰 Tecnologías Utilizadas

- **Node.js:** axios, dayjs, csv-writer  
- **Python:** pandas, re, unicodedata, melitk.bigquery (cliente BigQuery)  
- **BigQuery:** almacenamiento y consulta de datos finales  

---

## 📂 Estructura y Resumen del Código

### 1. Crawler (crawler.js)

- Importa módulos axios, dayjs y csv-writer  
- Define headers HTTP para simular navegador  
- Lista de categorías con IDs y nombres  
- Loop que hace petición API por cada categoría, guardando tiendas oficiales  
- Exporta datos completos a CSV con fecha en el nombre  

### 2. Post-Procesamiento (Python)

- Carga CSV en DataFrame pandas  
- Normaliza columnas `USERNAME_SHOPEE` y `BRAND_NAME_SHOPEE`:  
  - Mayúsculas, reemplazo de signos (`.`, `_`, `-`) por espacio  
  - Elimina signos de puntuación y acentos  
  - Sustituye `" & "` por `" E "`  
- Detecta caracteres especiales no permitidos y los reporta  
- Consulta tabla histórica en BigQuery para obtener IDs únicos existentes  
- Para marcas nuevas genera IDs alfanuméricos únicos (5 dígitos)  
- Actualiza DataFrame con `UNIC_ID`  
- Añade columnas de auditoría de inserción y actualización con timestamps  
- Convierte columna fecha scraping a formato datetime  
- Carga datos actualizados en tabla BigQuery para almacenamiento final  

---

## 📝 Campos Exportados

El CSV y la tabla BigQuery contienen:

- index  
- total  
- username  
- brand_name  
- shopid  
- logo  
- logo_pc  
- shop_collection_id  
- ctime  
- brand_label  
- shop_type  
- redirect_url  
- entity_id  
- category_id  
- category_name  
- url_to  
- data_requisicao  
- UNIC_ID  
- AUD_INS_DTTM  
- AUD_UPD_DTTM  
- DATE_SCRAPING  

---

## ✅ Cómo Usar

1. Clonar el repositorio  
2. Instalar dependencias Node.js:  
   ```bash
   npm install axios dayjs csv-writer
3. Ejecuta el script:
 `node crawler.js`  
5. El CSV será guardado automáticamente en la raíz del proyecto con la fecha del día en el nombre.
6. Ejecutar script Python para post-procesamiento y carga a BigQuery:
  `node input_tabla_crawler.py`
7. Verificar resultados y datos en BigQuery

## 📌 Notas

- La categoría con ID "-1" es la página principal con tiendas no categorizadas
- Se recomienda eliminar duplicados en post-procesamiento
- El proceso genera IDs únicos para las marcas nuevas, reutilizando las ya existentes


## 📎 Posibles Extensiones

- Integración directa con BigQuery para no generar csv 
- Automatizar la ejecución periódica con cron
- Mejorar limpieza y validación de datos
- Integración con dashboard para monitoreo en tiempo real
- Manejo avanzado de duplicados y consolidación de tiendas
