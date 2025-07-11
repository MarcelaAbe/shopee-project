# Coleta de Lojas Oficiais da Shopee por Categoria

Este projeto realiza a coleta de **lojas oficiais** da Shopee utilizando a API pública da plataforma. Os dados são organizados por categoria e exportados em um arquivo CSV com timestamp.

## 📦 Funcionalidades

- Faz requisições à API da Shopee por categoria  
- Emula headers de navegador real para evitar bloqueios  
- Traz categorias agrupadas e não agrupadas (como lojas novas)  
- Exporta os dados em formato CSV com timestamp  
- Permite adaptação para BigQuery ou outras bases de dados  

## 🧰 Tecnologias Utilizadas

- `axios`: para realizar requisições HTTP  
- `fs`: módulo nativo para manipulação de arquivos (não diretamente usado neste script)  
- `dayjs`: para manipulação de datas  
- `csv-writer`: para salvar os dados coletados em CSV  

## 📂 Estrutura do Script

### 1. Definição de Dependências

Importação dos módulos necessários e configuração dos headers para simular um navegador real.

### 2. Lista de Categorias

Dicionário com o mapeamento dos IDs de categoria e seus nomes legíveis.

### 3. Coleta de Dados

Loop por todas as categorias, com chamada à API para cada uma delas, salvando as informações relevantes das lojas.

### 4. Exportação

Criação de um arquivo CSV com o nome `brands_shopee_YYYYMMDD.csv`, contendo todos os dados coletados.

## 📝 Campos Exportados

O CSV gerado inclui os seguintes campos:

- `index`  
- `total`  
- `username`  
- `brand_name`  
- `shopid`  
- `logo`  
- `logo_pc`  
- `shop_collection_id`  
- `ctime`  
- `brand_label`  
- `shop_type`  
- `redirect_url`  
- `entity_id`  
- `category_id`  
- `category_name`  
- `url_to`  
- `data_requisicao`  

## ✅ Como Usar

1. Clone este repositório  
2. Instale as dependências:  
   `npm install axios dayjs csv-writer` 
3. Execute o script:  
   `node crawler.js`  
4. O CSV será salvo automaticamente na raiz do projeto com a data do dia no nome.

## 📌 Observações

- A categoria com ID "-1" representa a Página Principal, contendo lojas que ainda não foram oficialmente categorizadas.  
- Algumas duplicidades podem surgir. Recomenda-se tratá-las em pós-processamento conforme necessário.

## 📎 Possíveis Extensões

- Integração direta com BigQuery  
- Deduplicação automática  
- Agendamento via cron para coleta periódica

# Espanhol:

# Recolección de Tiendas Oficiales de Shopee por Categoría

Este proyecto realiza la recolección de **tiendas oficiales** de Shopee utilizando la API pública de la plataforma. Los datos se organizan por categoría y se exportan en un archivo CSV con timestamp.

## 📦 Funcionalidades

- Hace solicitudes a la API de Shopee por categoría  
- Emula headers de un navegador real para evitar bloqueos  
- Trae categorías agrupadas y no agrupadas (como tiendas nuevas)  
- Exporta los datos en formato CSV con timestamp  
- Permite adaptación para BigQuery u otras bases de datos  

## 🧰 Tecnologías Utilizadas

- `axios`: para realizar solicitudes HTTP  
- `fs`: módulo nativo para manipular archivos (no se usa directamente en este script)  
- `dayjs`: para trabajar con fechas  
- `csv-writer`: para guardar los datos recolectados en CSV  

## 📂 Estructura del Script

### 1. Definición de Dependencias

Importación de los módulos necesarios y configuración de los headers para simular un navegador real.

### 2. Lista de Categorías

Diccionario con el mapeo de los IDs de categoría y sus nombres legibles.

### 3. Recolección de Datos

Loop por todas las categorías, haciendo una llamada a la API por cada una y guardando la info relevante de las tiendas.

### 4. Exportación

Creación de un archivo CSV con el nombre `brands_shopee_YYYYMMDD.csv`, que contiene todos los datos recolectados.

## 📝 Campos Exportados

El CSV generado incluye los siguientes campos:

- `index`  
- `total`  
- `username`  
- `brand_name`  
- `shopid`  
- `logo`  
- `logo_pc`  
- `shop_collection_id`  
- `ctime`  
- `brand_label`  
- `shop_type`  
- `redirect_url`  
- `entity_id`  
- `category_id`  
- `category_name`  
- `url_to`  
- `data_requisicao`  

## ✅ Cómo Usar

1. Clona este repositorio  
2. Instala las dependencias:  
   `npm install axios dayjs csv-writer` 
3. Ejecuta el script:  
   `node crawler.js`  
4. El CSV será guardado automáticamente en la raíz del proyecto con la fecha del día en el nombre.

## 📌 Notas

- La categoría con ID "-1" representa la Página Principal, que contiene tiendas que aún no han sido categorizadas oficialmente.  
- Pueden surgir duplicados. Se recomienda tratarlos en el post-procesamiento si es necesario.

## 📎 Posibles Extensiones

- Integración directa con BigQuery  
- Eliminación automática de duplicados  
- Agendamiento con cron para recolectar periódicamente
