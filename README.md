# Recolección de Tiendas Oficiales de Shopee por Categoría

Este proyecto automatiza la recolección de datos de **tiendas oficiales** de Shopee, segmentadas por categoría, y su posterior procesamiento para normalización, comparación com dados do Mercado Livre e carga en BigQuery.

---

## 🤖 Resumen General

El proceso consta de tres etapas principales:

1. **Crawler en Node.js**  
   Consulta la API pública de Shopee por categoría para obtener tiendas oficiales. Los datos se recolectan en formato JSON y se exportan a CSV con timestamp.

2. **Tratamiento y Normalización en Python**  
   El CSV generado se limpia, normaliza, se generan IDs únicos (`UNIC_ID`) y se cruza con datos históricos en BigQuery.

3. **Matching com Dados do Mercado Livre**  
   Usa modelo BERT e regras heurísticas para comparar os nomes das lojas Shopee com dados de marcas do Mercado Livre, sem depender de uma chave única.

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
- Convierte columna fecha scraping a formato datetime  
- Elimina duplicatas com base en `(USERNAME_SHOPEE, BRAND_NAME_SHOPEE)`  
- Gera chave composta `MATCH_KEY` para rastreabilidade e controle de unicidade  
- Identifica e trata colisões de `UNIC_ID` entre marcas distintas  
- Tabela final é preparada para particionamento por `DATE_SCRAPING`  
- Realiza merge (upsert) dos dados no BigQuery com base no `UNIC_ID`, preservando o histórico

### Matching com Dados do Mercado Livre

- Usa modelo **BERT (RoBERTa)** para comparação semântica dos nomes de marca  
- Complementa com **regras heurísticas** para tratar abreviações, kits, variações e ruído  
- Não depende de uma chave exata de comparação  
- Gera um mapeamento probabilístico entre lojas da Shopee e marcas do Meli  
- Ajuda na consolidação e unificação de marcas para análises posteriores  

---

## 🧰 Tecnologías Utilizadas

- **Node.js:** axios, dayjs, csv-writer  
- **Python:** pandas, re, unicodedata, melitk.bigquery, sentence-transformers  
- **BigQuery:** almacenamiento y consulta de datos finales  
- **Modelos de NLP:** RoBERTa/BERT para correspondência semântica  

---

## 📂 Estructura y Resumen del Código

### 1. Crawler (crawler.js)

- Importa módulos axios, dayjs y csv-writer  
- Define headers HTTP para simular navegador  
- Lista de categorías con IDs y nombres  
- Loop que hace petición API por cada categoría, guardando tiendas oficiales  
- Exporta datos completos a CSV con fecha en el nombre  

### 2. Post-Procesamiento (Python)

- Carga CSV em DataFrame pandas  
- Normaliza `USERNAME_SHOPEE` y `BRAND_NAME_SHOPEE`  
  - Mayúsculas, remoção de sinais, acentos e caracteres duplicados  
  - Substituição de `&` por `E`  
- Gera `UNIC_ID` com base em marcas históricas no BigQuery  
- Gera `MATCH_KEY` único  
- Remove duplicatas  
- Detecta colisões de `UNIC_ID`  
- Adiciona colunas de auditoria (`AUD_INS_DTTM`, `AUD_UPD_DTTM`)  
- Realiza merge/upsert com BigQuery  
- Particiona por `DATE_SCRAPING`

### 3. Matching com Mercado Livre (Input_e_match.py)

- Carrega dados do Meli e Shopee  
- Normaliza todos os nomes e marcas  
- Aplica modelo **BERT (RoBERTa)** para gerar embeddings dos nomes  
- Calcula similaridade semântica entre marcas Shopee e Meli  
- Complementa com regras (kits, abreviações, quantidades, etc.)  
- Classifica os matches por nível de confiança (Alto, Médio, Baixo)  
- Exporta os matches e atualiza mapeamento para uso posterior  

---

## 📝 Campos Exportados

O CSV e a tabela BigQuery contêm:

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
- MATCH_KEY  
- AUD_INS_DTTM  
- AUD_UPD_DTTM  
- DATE_SCRAPING  

---

## ✅ Cómo Usar

1. Clonar el repositorio  
2. Instalar dependencias Node.js:  
   ```bash
   npm install axios dayjs csv-writer
   ```
3. Ejecutar el crawler:
   ```bash
   node crawler.js
   ```
4. Executar o tratamento e input de tablas:
   ```bash
   python input_tabla_scraper.py
   ```
5. Verificar os dados na tabela BigQuery resultante
6. Executar o tratamento e matching:
   ```bash
   python input_e_match.py
   ```

---

## 📌 Notas

- A categoria com ID `-1` representa a página principal com lojas não categorizadas  
- Todos os nomes são padronizados antes da comparação para aumentar a eficácia do matching  
- A combinação de BERT + regras garante cobertura mais robusta mesmo sem chave de união  
- IDs únicos (`UNIC_ID`) são preservados entre execuções para garantir rastreabilidade  
