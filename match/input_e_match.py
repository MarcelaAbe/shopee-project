#  --- Librerías esenciales para manipulación de datos y conexión ---
from datetime import datetime
import pandas as pd
import re
import unicodedata
import torch
from sentence_transformers import SentenceTransformer, util
from melitk.bigquery import BigQueryDatameshClientBuilder, BigQueryClientBuilderError
import numpy as np


# --- Paso 1: Importar la base de datos del crawler de Shopee ---
print("✨ Iniciando el proceso: importando datos de Shopee y Mercado Libre...")

# Inicializar df_old vacío para evitar errores si la consulta falla
# Es una buena práctica asegurar que la variable exista antes de usarse.
df_old = pd.DataFrame()

# Conectar a BigQuery para obtener datos de Shopee
try:
    # Construir el cliente de BigQuery para acceder a la DME de Shopee
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()
    print("✅ Conexión con BigQuery para Shopee establecida. Procediendo a la consulta de datos.")

    # Consulta para obtener los datos de Shopee. Se filtra por una fecha específica.
    query_shopee = """SELECT * FROM ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS
    WHERE DATE(DATE_SCRAPING) = DATE '2025-06-04'
    """

    # Ejecutar la consulta y cargar el resultado en un DataFrame de pandas
    response_shopee = bigquery_client.query_to_df(query_shopee)
    df_api = response_shopee.df  # Este es nuestro DataFrame de Shopee.

    print("✅ Acceso a la tabla de Shopee confirmado. Primeros registros:")
    print(df_api.head())
    print("\nColumnas obtenidas:")
    print(df_api.columns)

except BigQueryClientBuilderError as e:
    print(f"Error al construir el cliente de BigQuery para Shopee: {e}")
except Exception as e:
    print(f"Error inesperado al obtener los datos de Shopee: {e}")

# --- Paso 2: Importar la base de datos de Tiendas Oficiales (TOs) de Mercado Libre ---

# Inicializar df_old nuevamente (para asegurar que no haya errores si esta consulta falla)
df_old = pd.DataFrame()

# Conectar a BigQuery para obtener datos de Mercado Libre
try:
    # Reconstruir el cliente de BigQuery (se puede reutilizar, pero aquí se separa para claridad)
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()
    print("\n✅ Conexión con BigQuery para Mercado Libre establecida. Obteniendo datos de TOs.")

    # Consulta para obtener las Tiendas Oficiales de Mercado Libre, incluyendo IDs de categoría.
    query_meli = """
    WITH Category_ID_Table AS (
      SELECT
          OFS_OFFICIAL_STORE_ID,
          SIT_SITE_ID,
          OFS_NAME,
          OFS_FANTASY_NAME,
          OFS_STATUS,
          AUD_UPD_DTTM,
          CAST(REGEXP_REPLACE((SELECT REGEXP_REPLACE(category, r'[^0-9]', '') FROM UNNEST(OFS_CATEGORIES) AS category LIMIT 1), r'[^0-9]', '') AS INT64) AS Category_ID
      FROM
          meli-bi-data.WHOWNER.LK_OFS_OFFICIAL_STORES
      WHERE
          SIT_SITE_ID = 'MLB'
    ),

    Categories_Table AS (
      SELECT
          CAT_CATEG_ID_L1,
          CAT_CATEG_NAME_L1
      FROM
          meli-bi-data.WHOWNER.LK_CAT_AG_CATEGORIES
      WHERE
          SIT_SITE_ID = 'MLB'
    )

    SELECT DISTINCT
        c.OFS_OFFICIAL_STORE_ID,
        c.SIT_SITE_ID,
        c.OFS_NAME,
        c.OFS_FANTASY_NAME,
        c.OFS_STATUS,
        c.AUD_UPD_DTTM,
        c.Category_ID,
        t.CAT_CATEG_ID_L1,
        t.CAT_CATEG_NAME_L1
    FROM
        Category_ID_Table c
    LEFT JOIN
        Categories_Table t ON c.Category_ID = t.CAT_CATEG_ID_L1;
    """

    # Ejecutar la consulta y cargar el resultado en un DataFrame de pandas
    response_meli = bigquery_client.query_to_df(query_meli)
    df_meli = response_meli.df  # Este es nuestro DataFrame de Mercado Libre.

    print("✅ Acceso a la tabla de Mercado Libre confirmado. Primeros registros:")
    print(df_meli.head())
    print("\nColumnas disponibles:")
    print(df_meli.columns)

except BigQueryClientBuilderError as e:
    print(f"Error al construir el cliente de BigQuery para Mercado Libre: {e}")
except Exception as e:
    print(f"Error inesperado al obtener los datos de Mercado Libre: {e}")

# --- Paso 3: Procesamiento de la base de Mercado Libre para comparación con Shopee ---
print("\n🧹 Iniciando la limpieza y estandarización de los datos de Mercado Libre...")

# Reemplazar caracteres no deseados (., _, -) por espacios en los nombres de las tiendas
# Esto ayuda a normalizar los nombres para una mejor coincidencia.
df_meli['OFS_NAME'] = df_meli['OFS_NAME'].str.replace(r'[._-]', ' ', regex=True)
df_meli['OFS_FANTASY_NAME'] = df_meli['OFS_FANTASY_NAME'].str.replace(r'[._-]', ' ', regex=True)

print("Nombres de tiendas y nombres de fantasía de Mercado Libre después de la primera limpieza:")
print(df_meli)

# Función para la limpieza final de los nombres de marcas
def limpiar_nombre_marca(nombre):
    if pd.isna(nome):
        return nome

    # Eliminar caracteres específicos que no son útiles para la coincidencia
    nome = nome.replace('!', '')
    nome = nome.replace('?', '')
    nome = nome.replace('+', ' ')
    nome = nome.replace('@', '')
    nome = nome.replace('#', ' ')
    nome = nome.replace('(', '').replace(')', '')
    nome = nome.replace('_', ' ')
    nome = nome.replace("'", '')
    nome = nome.replace('|', '')
    nome = nome.replace(',', '')
    nome = nome.replace('/', '')
    nome = nome.replace('=', '')

    # Reemplazar "&" por " E " para estandarizar
    nome = re.sub(r'\s*&\s*', ' E ', nome)

    # Eliminar acentos (normalización Unicode)
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')

    # Eliminar espacios duplicados
    nome = re.sub(r'\s+', ' ', nome)

    # Eliminar espacios al inicio/final y convertir a mayúsculas
    return nome.strip().upper()

# Aplicar la función de limpieza a las columnas de nombres de Mercado Libre
df_meli['OFS_FANTASY_NAME'] = df_meli['OFS_FANTASY_NAME'].apply(limpiar_nome_marca)
df_meli['OFS_NAME'] = df_meli['OFS_NAME'].apply(limpiar_nome_marca)

print("\nNombres de tiendas y nombres de fantasía de Mercado Libre después de la limpieza completa:")
print(df_meli)

# Verificar si quedan caracteres especiales después de la limpieza
print("\n👀 Verificando si quedan caracteres especiales después de la limpieza...")
pattern = r'[^A-Za-z0-9 ]'  # Patrón para identificar cualquier carácter que no sea letra, número o espacio

username_invalidos = df_meli[df_meli['OFS_FANTASY_NAME'].fillna('').str.contains(pattern, regex=True)]
brand_invalidos = df_meli[df_meli['OFS_NAME'].fillna('').str.contains(pattern, regex=True)]

print("Caracteres especiales encontrados en OFS_FANTASY_NAME:")
print(username_invalidos[['OFS_FANTASY_NAME']])

print("\nCaracteres especiales encontrados en OFS_NAME:")
print(brand_invalidos[['OFS_NAME']])

# --- Paso 4: Manejo de TOs con registros duplicados en Mercado Libre ---
print("\n👯 Procesando los registros duplicados de TOs en Mercado Libre, seleccionando siempre el más reciente.")

# Contar cuántos registros existen por nombre y estado para identificar duplicidades
contagem = df_meli.groupby(['OFS_FANTASY_NAME', 'OFS_STATUS']).size().reset_index(name='COUNT')
total_por_nome = contagem.groupby('OFS_FANTASY_NAME')['COUNT'].sum().reset_index(name='TOTAL')
nomes_com_mais_de_um = total_por_nome[total_por_nome['TOTAL'] > 1]['OFS_FANTASY_NAME']

print("Nombres de fantasía de Mercado Libre con más de un registro (duplicados):")
print(contagem[contagem['OFS_FANTASY_NAME'].isin(nomes_com_mais_de_um)])

# Filtrar el DataFrame original para incluir solo los nombres repetidos
df_filtrado = df_meli[df_meli['OFS_FANTASY_NAME'].isin(nomes_com_mais_de_um)]

# Ordenar por la fecha de actualización para obtener el registro más reciente
df_ordenado = df_filtrado.sort_values(by='AUD_UPD_DTTM', ascending=False)

# Obtener el registro más reciente de cada combinación Nombre + Estado
df_mais_recentes = df_ordenado.drop_duplicates(subset=['OFS_FANTASY_NAME', 'OFS_STATUS'], keep='first')

print("\nRegistros más recientes para nombres de fantasía duplicados:")
print(df_mais_recentes[['OFS_FANTASY_NAME', 'OFS_STATUS', 'AUD_UPD_DTTM']])

# Consolidar: registros únicos más los más recientes de los duplicados
total_por_nome = df_meli.groupby('OFS_FANTASY_NAME').size().reset_index(name='TOTAL')
nomes_unicos = total_por_nome[total_por_nome['TOTAL'] == 1]['OFS_FANTASY_NAME']
nomes_duplicados = total_por_nome[total_por_nome['TOTAL'] > 1]['OFS_FANTASY_NAME']

df_unicos = df_meli[df_meli['OFS_FANTASY_NAME'].isin(nomes_unicos)]
df_duplicados = df_meli[df_meli['OFS_FANTASY_NAME'].isin(nomes_duplicados)]
df_duplicados = df_duplicados.sort_values(by='AUD_UPD_DTTM', ascending=False)
df_duplicados_mais_recentes = df_duplicados.drop_duplicates(subset=['OFS_FANTASY_NAME', 'OFS_STATUS'], keep='first')

df_final = pd.concat([df_unicos, df_duplicados_mais_recentes], ignore_index=True)
df_final = df_final.sort_values(by='OFS_FANTASY_NAME').reset_index(drop=True)

print(f'\nTotal de registros finales en Mercado Libre después de la deduplicación: {len(df_final)}')
print(df_final[['OFS_FANTASY_NAME', 'OFS_STATUS', 'AUD_UPD_DTTM']])

df_meli = df_final  # Nuestro DataFrame de Mercado Libre ahora está limpio y sin duplicados.
print("\nDataFrame de Mercado Libre finalizado después del procesamiento:")
print(df_meli)
print(df_meli.info())


# --- Paso 5: Realizar la coincidencia (Match) de la base de Shopee con la base de Mercado Libre ---
print("\n🔗 ¡Momento de la coincidencia! Cruzando Shopee y Mercado Libre para identificar las tiendas.")

# DataFrame de trabajo con UNIC_ID de Shopee
df_trabalho = df_api[['UNIC_ID', 'BRAND_NAME_SHOPEE', 'USERNAME_SHOPEE']].copy()

# Función auxiliar para realizar el merge y registrar el tipo de coincidencia
def encontrar_e_merge(df_busca, chave_busca, chave_meli):
    df_temp = df_busca.merge(df_meli, left_on=chave_busca, right_on=chave_meli, how='inner')
    df_temp['VALOR_ENCONTRADO'] = chave_busca  # Valor de Shopee que encontró coincidencia
    df_temp['COLUNA_ENCONTRADA_EM'] = chave_meli  # Columna de Mercado Libre donde se encontró la coincidencia
    return df_temp

# Secuencia de intentos de coincidencia: primero BRAND_NAME_SHOPEE vs OFS_FANTASY_NAME,
# luego BRAND_NAME_SHOPEE vs OFS_NAME, y así sucesivamente, trabajando con los registros restantes.
df1 = encontrar_e_merge(df_trabalho, 'BRAND_NAME_SHOPEE', 'OFS_FANTASY_NAME')
rest2 = df_trabalho[~df_trabalho['UNIC_ID'].isin(df1['UNIC_ID'])]

df2 = encontrar_e_merge(rest2, 'BRAND_NAME_SHOPEE', 'OFS_NAME')
rest3 = rest2[~rest2['UNIC_ID'].isin(df2['UNIC_ID'])]

df3 = encontrar_e_merge(rest3, 'USERNAME_SHOPEE', 'OFS_FANTASY_NAME')
rest4 = rest3[~rest3['UNIC_ID'].isin(df3['UNIC_ID'])]

df4 = encontrar_e_merge(rest4, 'USERNAME_SHOPEE', 'OFS_NAME')

# Identificar las tiendas que no encontraron coincidencia en ninguno de los pasos anteriores
ids_encontrados = pd.concat([df1, df2, df3, df4])['UNIC_ID'].unique()
df_nao_encontrado = df_trabalho[~df_trabalho['UNIC_ID'].isin(ids_encontrados)].copy()
df_nao_encontrado['VALOR_ENCONTRADO'] = 'NÃO_ENCONTRADO'  # Marcar claramente
df_nao_encontrado['COLUNA_ENCONTRADA_EM'] = 'NÃO_ENCONTRADO'  # Y dónde no se encontró

# Añadir columnas de Mercado Libre con valores vacíos para las tiendas no encontradas
colunas_meli = [
    'OFS_OFFICIAL_STORE_ID', 'SIT_SITE_ID', 'OFS_NAME', 'OFS_FANTASY_NAME',
    'OFS_STATUS', 'Category_ID', 'CAT_CATEG_ID_L1', 'CAT_CATEG_NAME_L1'
]
for col in colunas_meli:
    df_nao_encontrado[col] = pd.NA

# Unir todos los resultados (coincidencias y no coincidencias)
df_merged = pd.concat([df1, df2, df3, df4, df_nao_encontrado], ignore_index=True)

# Realizar un merge final con df_api para incluir todas las columnas originales de Shopee
df_resultado = df_api.merge(df_merged.drop(columns=['BRAND_NAME_SHOPEE', 'USERNAME_SHOPEE'], errors='ignore'),
                            on='UNIC_ID', how='left')

# Reorganizar las columnas para una mejor visualización
colunas_api = [col for col in df_api.columns if col in df_resultado.columns]
colunas_info = ['VALOR_ENCONTRADO', 'COLUNA_ENCONTRADA_EM']
colunas_meli_presentes = [col for col in colunas_meli if col in df_resultado.columns]
colunas_finais = colunas_api + colunas_info + colunas_meli_presentes
df_resultado = df_resultado[colunas_finais]

# Evaluar el resultado por tienda (USERNAME_SHOPEE)
df_lojas = df_resultado.groupby('USERNAME_SHOPEE')['COLUNA_ENCONTRADA_EM'].apply(
    lambda x: any(x != 'NÃO_ENCONTRADO')  # Verificar si al menos un ítem de la tienda fue encontrado
).reset_index(name='ENCONTRADA')

# Estadísticas de la coincidencia
total_lojas_unicas = df_lojas['USERNAME_SHOPEE'].nunique()
total_encontradas = df_lojas['ENCONTRADA'].sum()
total_nao_encontradas = total_lojas_unicas - total_encontradas

print(f"\n📊 Resumen de la Coincidencia Directa:")
print(f"Total de tiendas únicas en Shopee: {total_lojas_unicas}")
print(f"Tiendas con al menos una coincidencia: {total_encontradas}")
print(f"Tiendas sin coincidencia: {total_nao_encontradas}")

# Verificar si alguna tienda "desapareció" (no debería ocurrir)
faltando = set(df_api['USERNAME_SHOPEE']) - set(df_resultado['USERNAME_SHOPEE'])
print(f'Tiendas que no están en el resultado final (debería ser 0): {len(faltando)}')
if faltando:
    print('Ejemplos:', list(faltando)[:10])

print("\nVista previa del DataFrame de resultado después de la coincidencia directa:")
print(df_resultado.head())

# Agrupar el resultado por tienda e indicar si fue encontrada o no
df_lojas = df_resultado.groupby('USERNAME_SHOPEE')['COLUNA_ENCONTRADA_EM'].apply(
    lambda x: any(x != 'NÃO_ENCONTRADO')
).reset_index(name='ENCONTRADA')

# Añadir columna STATUS para facilitar la visualización
df_lojas['STATUS'] = df_lojas['ENCONTRADA'].apply(lambda x: 'ENCONTRADA' if x else 'NÃO_ENCONTRADA')

# Separar las tiendas no encontradas para el siguiente paso
df_lojas_nao_encontradas_direto = df_lojas[df_lojas['STATUS'] == 'NÃO_ENCONTRADA']

print("\nEstadísticas de tiendas únicas después de la coincidencia directa:")
print(f"Total de tiendas únicas en Shopee: {total_lojas_unicas}")
print(f"Tienda(s) única(s) encontrada(s): {total_encontradas}")
print(f"Tienda(s) única(s) no encontrada(s): {total_nao_encontradas}")

print("\nLista de las tiendas de Shopee que *aún no* han sido encontradas:")
print(df_lojas_nao_encontradas_direto['USERNAME_SHOPEE'].dropna().tolist())

# --- Paso 6: Complementar la coincidencia con contexto BERT (IA) ---
print("\n🧠 Utilizando inteligencia artificial (BERT) para encontrar coincidencias más complejas.")

# Cargar el modelo BERT pre-entrenado (ligero y eficiente)
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# Filtrar solo las tiendas de Shopee que *no fueron encontradas* en la coincidencia directa
df_nao_encontrado_para_bert = df_resultado[
    (df_resultado['VALOR_ENCONTRADO'] == 'NÃO_ENCONTRADO') &
    (df_resultado['COLUNA_ENCONTRADA_EM'] == 'NÃO_ENCONTRADO')
].copy()

# Crear la lista de nombres de tiendas de Shopee para el modelo BERT
lista_shopee_bert = df_nao_encontrado_para_bert['USERNAME_SHOPEE'].dropna().drop_duplicates().tolist()

# Crear la lista de nombres de Mercado Libre para ser los "candidatos" a la coincidencia
# Se incluyen tanto el nombre oficial como el nombre de fantasía.
candidatos = []
for col in ['OFS_NAME', 'OFS_FANTASY_NAME']:
    for idx, val in df_meli[col].dropna().astype(str).items():
        candidatos.append((val.strip(), idx, col))  # Almacenar texto, índice original y la columna

lista_candidatos_texto = [c[0] for c in candidatos]  # Solo el texto para el modelo

# Generar los embeddings (representaciones numéricas) para Shopee y Mercado Libre
embeddings_shopee = model.encode(lista_shopee_bert, convert_to_tensor=True)
embeddings_meli = model.encode(lista_candidatos_texto, convert_to_tensor=True)

# Definir el umbral de similitud (0.80 es un buen punto de partida)
limiar_similaridade = 0.80

matches_bert = []

# Comparar cada tienda no encontrada de Shopee con todas las de Mercado Libre
for i, embedding_shopee in enumerate(embeddings_shopee):
    similaridades = util.pytorch_cos_sim(embedding_shopee, embeddings_meli)[0]
    top_score, top_idx = torch.max(similaridades, dim=0)  # Obtener la mayor similitud

    if float(top_score) >= limiar_similaridade:  # Si está por encima del umbral, es una coincidencia
        texto_match, idx_df_meli, coluna_encontrada = candidatos[top_idx]
        row_meli = df_meli.loc[idx_df_meli]  # Obtener la fila completa de Mercado Libre

        matches_bert.append({
            'USERNAME_SHOPEE': lista_shopee_bert[i],
            'MATCH_MELI': texto_match,
            'SIMILARITY': float(top_score),
            'COLUNA_ENCONTRADA_EM': coluna_encontrada,
            'OFS_OFFICIAL_STORE_ID': row_meli.get('OFS_OFFICIAL_STORE_ID'),
            'SIT_SITE_ID': row_meli.get('SIT_SITE_ID'),
            'OFS_NAME': row_meli.get('OFS_NAME'),
            'OFS_FANTASY_NAME': row_meli.get('OFS_FANTASY_NAME'),
            'OFS_STATUS': row_meli.get('OFS_STATUS'),
            'Category_ID': row_meli.get('Category_ID'),
            'CAT_CATEG_ID_L1': row_meli.get('CAT_CATEG_ID_L1'),
            'CAT_CATEG_NAME_L1': row_meli.get('CAT_CATEG_NAME_L1')
        })

df_matches_bert = pd.DataFrame(matches_bert)

print("\nResultados de la Coincidencia Semántica (BERT) con similitud >= 0.80:")
print(df_matches_bert)

# Actualizar el DataFrame de resultados con las coincidencias encontradas por BERT
df_resultado_atualizado = df_resultado.copy()

for _, row in df_matches_bert.iterrows():
    # Encontrar las filas en df_resultado_atualizado que corresponden a la tienda de Shopee no encontrada
    idx = df_resultado_atualizado[
        (df_resultado_atualizado['USERNAME_SHOPEE'] == row['USERNAME_SHOPEE']) &
        (df_resultado_atualizado['VALOR_ENCONTRADO'] == 'NÃO_ENCONTRADO') &
        (df_resultado_atualizado['COLUNA_ENCONTRADA_EM'] == 'NÃO_ENCONTRADO')
    ].index

    # Si se encontró, actualizar la información de la coincidencia
    if not idx.empty:
        i = idx[0]  # Tomar el primer índice (puede haber más de un ítem por tienda)
        df_resultado_atualizado.at[i, 'VALOR_ENCONTRADO'] = row['MATCH_MELI']
        df_resultado_atualizado.at[i, 'COLUNA_ENCONTRADA_EM'] = row['COLUNA_ENCONTRADA_EM']
        df_resultado_atualizado.at[i, 'OFS_OFFICIAL_STORE_ID'] = row['OFS_OFFICIAL_STORE_ID']
        df_resultado_atualizado.at[i, 'SIT_SITE_ID'] = row['SIT_SITE_ID']
        df_resultado_atualizado.at[i, 'OFS_NAME'] = row['OFS_NAME']
        df_resultado_atualizado.at[i, 'OFS_FANTASY_NAME'] = row['OFS_FANTASY_NAME']
        df_resultado_atualizado.at[i, 'OFS_STATUS'] = row['OFS_STATUS']
        df_resultado_atualizado.at[i, 'Category_ID'] = row['Category_ID']
        df_resultado_atualizado.at[i, 'CAT_CATEG_ID_L1'] = row['CAT_CATEG_ID_L1']
        df_resultado_atualizado.at[i, 'CAT_CATEG_NAME_L1'] = row['CAT_CATEG_NAME_L1']

print("\nDataFrame de resultado actualizado después de la coincidencia con BERT:")
print(df_resultado_atualizado)


# --- Paso 7: Importar datos de validación manual de Sheets ---
print("\n📄 Importando datos de Sheets para revisar el trabajo de verificación manual.")

# Inicializar df_old (práctica recomendada)
df_old = pd.DataFrame()

# Conectar a BigQuery para obtener los datos de Sheets
try:
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()
    print("✅ Conexión con BigQuery para Sheets establecida.")

    query_sheets = """SELECT * FROM ddme000418-dn88p8g386x-furyid.TBL.DM_STEP_PLAN_ACTION_TO_SHOPEE_PH
    """

    response_sheets = bigquery_client.query_to_df(query_sheets)
    df_sheets = response_sheets.df  # Nuestro DataFrame con los datos de Sheets.

    print("✅ Acceso a la tabla de Sheets confirmado!")
    print(df_sheets.head())
    print(df_sheets.columns)

except BigQueryClientBuilderError as e:
    print(f"Error al construir el cliente de BigQuery para Sheets: {e}")
except Exception as e:
    print(f"Error inesperado al obtener los datos de Sheets: {e}")

# Estandarizar el nombre de la tienda en Sheets
df_sheets['LOJA_OFICIAL_SHOPEE'] = df_sheets['LOJA_OFICIAL_SHOPEE'].str.upper()
print("\nNombre de la tienda en Sheets en mayúsculas:")
print(df_sheets)

# Limpieza adicional en los nombres de Shopee de Sheets
df_sheets['LOJA_OFICIAL_SHOPEE'] = df_sheets['LOJA_OFICIAL_SHOPEE'].str.replace(r'[._-]', ' ', regex=True)
print("\nNombre de la tienda en Sheets después de la limpieza de caracteres:")
print(df_sheets)

# Aplicar la función de limpieza previamente definida (reutilización de código)
df_sheets['LOJA_OFICIAL_SHOPEE'] = df_sheets['LOJA_OFICIAL_SHOPEE'].apply(limpiar_nome_marca)
print("\nNombre de la tienda en Sheets después de la limpieza completa (sin acentos y estandarizado):")
print(df_sheets)

# Verificar si quedan caracteres especiales en Sheets
print("\n👀 Verificando si quedan caracteres especiales en los nombres de Sheets...")
username_invalidos_sheets = df_sheets[df_sheets['LOJA_OFICIAL_SHOPEE'].fillna('').str.contains(pattern, regex=True)]
print("Caracteres especiales encontrados en LOJA_OFICIAL_SHOPEE (Sheets):")
print(username_invalidos_sheets[['LOJA_OFICIAL_SHOPEE']])

# --- Paso 8: Actualizar resultados con la coincidencia manual de Sheets ---
print("\n✋ Combinando los resultados automáticos con la coincidencia manual de Sheets.")

# Filtrar las tiendas que fueron marcadas como "Encontrado" en Sheets (validación manual)
df_encontrados_manual = df_sheets.loc[
    (df_sheets['MATCH_SELECTION'].eq('Encontrado')) |
    (df_sheets['MATCH_COMERCIAL'].eq('Encontrado')),
    'LOJA_OFICIAL_SHOPEE'
].dropna().str.strip().str.upper()

# Usar un set para una comparación más eficiente
lojas_encontradas_manual = set(df_encontrados_manual)

# Máscara para identificar las filas que *aún no* han sido encontradas por los métodos automáticos (directo o BERT)
mask_nao_encontrado_total = (
    df_resultado_atualizado['VALOR_ENCONTRADO'].eq('NÃO_ENCONTRADO') |
    df_resultado_atualizado['COLUNA_ENCONTRADA_EM'].eq('NÃO_ENCONTRADO')
)

# Columna de Shopee para la comparación (USERNAME_SHOPEE es un buen identificador)
col_loja = 'USERNAME_SHOPEE'

# Verificar si la tienda de Shopee (que no fue encontrada previamente) está en la lista manual
mask_loja_bateu_manual = (
    df_resultado_atualizado[col_loja]
      .fillna('')
      .str.strip()
      .str.upper()
      .isin(lojas_encontradas_manual)
)

# Actualizar las columnas de coincidencia a 'ENCONTRADO_MANUAL' si ambas condiciones son verdaderas
mask_final_manual = mask_nao_encontrado_total & mask_loja_bateu_manual

df_resultado_atualizado.loc[mask_final_manual, ['VALOR_ENCONTRADO', 'COLUNA_ENCONTRADA_EM']] = 'ENCONTRADO_MANUAL'

print(f"Filas actualizadas con coincidencia manual: {mask_final_manual.sum()}")
print("\nDataFrame final después de aplicar la coincidencia manual:")
print(df_resultado_atualizado)
print(df_resultado_atualizado.info())

# --- Paso 9: Renombrar y ajustar columnas para BigQuery ---
print("\n✏️ Organizando los nombres de las columnas y preparando todo para BigQuery.")

# Renombrar las columnas al estándar final, más claro
df_resultado_atualizado.rename(columns={
    'SHOPID': 'SHOPID_SHOPEE',
    'LOGO': 'LOGO_SHOPEE',
    'LOGO_PC': 'LOGO_PC_SHOPEE',
    'SHOP_COLLECTION_ID': 'SHOP_COLLECTION_ID_SHOPEE',
    'CTIME': 'CTIME_SHOPEE',
    'BRAND_LABEL': 'BRAND_LABEL_SHOPEE',
    'SHOP_TYPE': 'SHOP_TYPE_SHOPEE',
    'REDIRECT_URL': 'REDIRECT_URL_SHOPEE',
    'ENTITY_ID': 'ENTITY_ID_SHOPEE',
    'CATEGORY_ID': 'CATEGORY_ID_SHOPEE',
    'CATEGORY_NAME': 'CATEGORY_NAME_SHOPEE',
    'TO_URL': 'TO_URL_SHOPEE',
    'UNIC_ID': 'UNIC_ID_SHOPEE',
    'FIRST_APPEARENCE': 'FIRST_APPEARENCE_SHOPEE',
    'OFS_OFFICIAL_STORE_ID': 'OFFICIAL_STORE_ID_MELI',
    'SIT_SITE_ID': 'SIT_SITE_ID_MELI',
    'OFS_NAME': 'LOJA_OFICIAL_MELI',
    'OFS_FANTASY_NAME': 'FANTASY_NAME_MELI',
    'OFS_STATUS': 'OFS_STATUS_MELI',
    'Category_ID': 'CATEGORIES_MELI_ID',
    'CAT_CATEG_ID_L1': 'CATEGORY_ID_L1_MELI',
    'CAT_CATEG_NAME_L1': 'CATEGORY_NAME_L1_MELI'
}, inplace=True)

# Eliminar la columna AUD_INS_DTTM, que se volverá a crear al final
df_resultado_atualizado.drop(columns=['AUD_INS_DTTM'], inplace=True)

# Marcar si la tienda de Shopee es "nueva" (apareció en la fecha de scraping actual)
# Es importante actualizar esta fecha si el scraping corresponde a otro día.
# Considerar una mejora para obtener esta fecha de forma automática.
df_resultado_atualizado['LOJA_NOVA_SHOPEE'] = df_resultado_atualizado['FIRST_APPEARENCE_SHOPEE'].dt.date == pd.to_datetime("2025-06-04").date()
print("\nValores únicos para 'LOJA_NOVA_SHOPEE':")
print(df_resultado_atualizado['LOJA_NOVA_SHOPEE'].unique())


# Añadir la fecha en que se realizó la coincidencia (hoy)
df_resultado_atualizado['DATE_MATCH'] = pd.to_datetime(datetime.today().date())

# Indicar si la coincidencia fue realizada por IA (automática o BERT)
df_resultado_atualizado['FOUND_BY_AI'] = ~(
    (df_resultado_atualizado['VALOR_ENCONTRADO'] == 'NÃO_ENCONTRADO') |
    (df_resultado_atualizado['COLUNA_ENCONTRADA_EM'] == 'NÃO_ENCONTRADO') |
    (df_resultado_atualizado['VALOR_ENCONTRADO'] == 'ENCONTRADO_MANUAL')  # Excluir las encontradas manualmente
)
print("\nValores para 'FOUND_BY_AI':")
print(df_resultado_atualizado['FOUND_BY_AI'])

# Renombrar las columnas de coincidencia a nombres más descriptivos
df_resultado_atualizado = df_resultado_atualizado.rename(columns={
    'VALOR_ENCONTRADO': 'MATCH_VALUE_USED',
    'COLUNA_ENCONTRADA_EM': 'MATCH_COLUMN_MELI'
})

# --- Paso 10: Clasificación de verticales con BERT (para los datos de Mercado Libre) ---
print("\nCategorizando las tiendas de Mercado Libre en verticales con el poder de BERT.")

# Cargar un modelo BERT multilingüe (ideal para español)
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

# Nuestros segmentos de verticales definidos
verticals = [
    "SPORTS", "T & B", "ENTERTAINMENT", "CONSTRUCTION & INDUSTRY",
    "HOME ELECTRONICS", "TECHNOLOGY", "OTHERS", "VEHICLE PARTS & ACCESSORIES",
    "FASHION", "BEAUTY", "HEALTH", "FURNISHING & HOUSEWARE", "CPG"
]

# Generar embeddings para los nombres de las verticales
vertical_embeddings = model.encode(verticals, convert_to_tensor=True)

# Mapeo manual para categorías que ya tienen una vertical clara
mapeamento_manual = {
    "Alimentos e Bebidas": "CPG",
    "Brinquedos e Hobbies": "T & B",
    "Eletônicos, Áudio e Vídeo": "HOME ELECTRONICS",
    "Eletrônicos, Áudio e Vídeo": "HOME ELECTRONICS",
    "Saúde": "HEALTH",
    "Beleza e Cuidado Pessoal": "BEAUTY",
    "Casa, Móveis e Decoração": "FURNISHING & HOUSEWARE",
    "Calçados, Roupas e Bolsas": "FASHION"
}

# Función para clasificar la vertical de una categoría
def classificar_vertical(category_name):
    if pd.isna(category_name):
        return "OTHERS"  # Si no hay nombre, se asigna a "Otros"

    # Primero, intentar el mapeo manual (es más rápido y preciso)
    if category_name in mapeamento_manual:
        return mapeamento_manual[category_name]

    # Si no hay mapeo manual, usar BERT para encontrar la similitud
    cat_embedding = model.encode(category_name, convert_to_tensor=True)
    scores = util.cos_sim(cat_embedding, vertical_embeddings)[0]
    best_idx = scores.argmax().item()  # Obtener el índice de la vertical más similar
    return verticals[best_idx]  # Retornar el nombre de la vertical

# Crear la columna VERTICAL_MELI si no existe
if "VERTICAL_MELI" not in df_resultado_atualizado.columns:
    df_resultado_atualizado["VERTICAL_MELI"] = None

# Aplicar la clasificación solo donde la vertical aún está vacía
mask_vazia = df_resultado_atualizado["VERTICAL_MELI"].isna()
df_resultado_atualizado.loc[mask_vazia, "VERTICAL_MELI"] = df_resultado_atualizado.loc[mask_vazia, "CATEGORY_NAME_SHOPEE"].apply(classificar_vertical)

print("\nVerticales únicas después de la clasificación:")
print(df_resultado_atualizado["VERTICAL_MELI"].unique())

# Crear la columna 'MAIN_CATEGORIES_MELI_ID' con valores nulos (el tipo Float64 acepta NaN)
df_resultado_atualizado['MAIN_CATEGORIES_MELI_ID'] = pd.Series([pd.NA] * len(df_resultado_atualizado), dtype='Float64')

# --- Paso 11: Validación del Esquema y Reorganización de Columnas ---
print("\n✅ Revisando el esquema y organizando las columnas, todo listo para BigQuery.")

# Definir el esquema esperado para BigQuery (nombre_columna: tipo_pandas)
schema_esperado = {
    'USERNAME_SHOPEE':              'object',
    'BRAND_NAME_SHOPEE':            'object',
    'SHOPID_SHOPEE':                'Int64',
    'LOGO_SHOPEE':                  'object',
    'LOGO_PC_SHOPEE':               'object',
    'SHOP_COLLECTION_ID_SHOPEE':    'Int64',
    'CTIME_SHOPEE':                 'Int64',
    'BRAND_LABEL_SHOPEE':           'Int64',
    'SHOP_TYPE_SHOPEE':             'Int64',
    'REDIRECT_URL_SHOPEE':          'object',
    'ENTITY_ID_SHOPEE':             'Int64',
    'CATEGORY_ID_SHOPEE':           'Int64',
    'CATEGORY_NAME_SHOPEE':         'object',
    'TO_URL_SHOPEE':                'object',
    'LOJA_NOVA_SHOPEE':             'bool',
    'FIRST_APPEARENCE_SHOPEE':      'datetime64[ns, UTC]',
    'UNIC_ID_SHOPEE':               'object',
    'DATE_SCRAPING':                'datetime64[ns, UTC]',
    'DATE_MATCH':                   'datetime64[ns]',
    'FOUND_BY_AI':                  'bool',
    'LOJA_OFICIAL_MELI':            'object',
    'FANTASY_NAME_MELI':            'object',
    'OFFICIAL_STORE_ID_MELI':       'object',
    'CATEGORIES_MELI_ID':           'Int64',
    'MAIN_CATEGORIES_MELI_ID':      'Float64',
    'VERTICAL_MELI':                'object',
    'SIT_SITE_ID_MELI':             'object',
    'MATCH_VALUE_USED':             'object',  # Nombre actualizado
    'MATCH_COLUMN_MELI':            'object',  # Nombre actualizado
    'OFS_STATUS_MELI':              'object',
    'CATEGORY_ID_L1_MELI':          'Int64',
    'CATEGORY_NAME_L1_MELI':        'object'
}

# Función auxiliar para obtener el tipo de dato como string
def dtype_str(dtype):
    if pd.api.types.is_datetime64_any_dtype(dtype):
        tz = getattr(dtype, 'tz', None)
        return f'datetime64[ns{", "+tz.zone if tz else ""}]'
    return str(dtype)

# Comparar columnas y tipos
colunas_df       = set(df_resultado_atualizado.columns)
colunas_esperado = set(schema_esperado.keys())

faltando = sorted(colunas_esperado - colunas_df)
extras   = sorted(colunas_df - colunas_esperado)

tipos_diferentes = []
for col in colunas_df & colunas_esperado:
    tipo_real = dtype_str(df_resultado_atualizado[col].dtype)
    tipo_exp  = schema_esperado[col]
    if tipo_real != tipo_exp:
        tipos_diferentes.append((col, tipo_real, tipo_exp))

print("\n--- Validación del Esquema ---")
print("▶ Columnas ausentes en el DataFrame:", faltando or "Ninguna")
print("▶ Columnas extra en el DataFrame:",   extras   or "Ninguna")
print("\n▶ Columnas con tipos diferentes:")
for col, real, exp in tipos_diferentes:
    print(f"  - {col}: tipo real={real} | tipo esperado={exp}")

# Orden final de las columnas para BigQuery
ordem_colunas = list(schema_esperado.keys())  # Obtener el orden de las claves del esquema

# Reorganizar el DataFrame con el orden correcto
df_resultado_atualizado = df_resultado_atualizado[ordem_colunas]

# --- Paso 12: Añadir Metadatos y Finalizar ---
print("\n🎉 ¡Todo listo! Añadiendo marcas de tiempo y finalizando el proceso ETL.")

# Crear las columnas de auditoría (fecha de inserción y actualización)
horario = datetime.now().isoformat()
df_resultado_atualizado['AUD_INS_DTTM'] = pd.to_datetime(horario)
df_resultado_atualizado['AUD_UPD_DTTM'] = pd.to_datetime(horario)

print("\n¡Proceso ETL finalizado!")
print("\nTipos de datos del DataFrame final:")
print(df_resultado_atualizado.dtypes)
print("\nDataFrame final listo para BigQuery:")
print(df_resultado_atualizado)

# --- Paso 13: Insertar los datos en BigQuery ---
print("\n🚀 ¡Es hora de enviar los datos a la tabla de BigQuery!")

try:
    # Construir el cliente de BigQuery nuevamente (siempre es bueno verificar)
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()

    print("Iniciando la inserción de datos en BigQuery...")

    # Nombre de la tabla en BigQuery
    table_id = 'ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS_MATCH_AT'

    # Configuraciones del trabajo: CREATE_NEVER (no crea la tabla si no existe) y 'append' (añade a los datos existentes)
    job_config_attributes = {
        "create_disposition": "CREATE_NEVER",
        "mode": "append"
    }

    # Insertar el DataFrame en BigQuery
    bigquery_client.df_to_gbq(df_resultado_atualizado, table_id, **job_config_attributes)

    print("¡Datos insertados con éxito en BigQuery! ¡Misión cumplida! 🎉")

except BigQueryClientBuilderError as builder_error:
    print(f"Error al construir el cliente de BigQuery: {builder_error}")
except Exception as e:
    print(f"Ocurrió un error inesperado al intentar insertar los datos: {e}")
