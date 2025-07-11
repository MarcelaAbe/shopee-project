import unicodedata
import re
import pandas as pd
from datetime import datetime
import pandas as pd
from melitk.bigquery import BigQueryDatameshClientBuilder, BigQueryClientBuilderError
import random
import string

# Poner en mayúsculas las primeras columnas de nombre
df_shopee['USERNAME_SHOPEE'] = df_shopee['USERNAME_SHOPEE'].str.upper()
df_shopee['BRAND_NAME_SHOPEE'] = df_shopee['BRAND_NAME_SHOPEE'].str.upper()

# Reemplazar ".", "_", "-" por espacio en las dos columnas
df_shopee['USERNAME_SHOPEE'] = df_shopee['USERNAME_SHOPEE'].str.replace(r'[._-]', ' ', regex=True)
df_shopee['BRAND_NAME_SHOPEE'] = df_shopee['BRAND_NAME_SHOPEE'].str.replace(r'[._-]', ' ', regex=True)

print(df_shopee)

def limpiar_nome_marca(nome):
    if pd.isna(nome):
        return nome

    # Reglas específicas para reemplazos directos
    nome = nome.replace('!', '')
    nome = nome.replace('(', '').replace(')', '')
    nome = nome.replace('_', ' ')
    nome = nome.replace("'", '')
    nome = nome.replace('|', '')
    nome = nome.replace(',', '')
    nome = nome.replace('/', '')
    nome = nome.replace('=', '')
    nome = nome.replace('>', '')
    nome = nome.replace('<', '')

    # Reemplazar " & " o variantes con espacio por " E "
    nome = re.sub(r'\s*&\s*', ' E ', nome)
    # Mantener & solo cuando esté entre letras y sin espacios (ej: P&P), eso ya está cubierto arriba

    # Sacar tildes y acentos en general
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('utf-8')

    # Quitar espacios repetidos
    nome = re.sub(r'\s+', ' ', nome)

    # Sacar espacios al principio y al final y pasar todo a MAYÚSCULAS
    return nome.strip().upper()

# Aplicar a la columna BRAND_NAME_SHOPEE
df_shopee['BRAND_NAME_SHOPEE'] = df_shopee['BRAND_NAME_SHOPEE'].apply(limpiar_nome_marca)

# Verificar si hay caracteres especiales (cualquier cosa que no sea letra, número o espacio)
pattern = r'[^A-Za-z0-9 ]'

# USERNAME_SHOPEE con caracteres especiales
username_invalidos = df_shopee[df_shopee['USERNAME_SHOPEE'].fillna('').str.contains(pattern, regex=True)]

# BRAND_NAME_SHOPEE con caracteres especiales
brand_invalidos = df_shopee[df_shopee['BRAND_NAME_SHOPEE'].fillna('').str.contains(pattern, regex=True)]

# Mostrar resultados
print("Carácter especial encontrado en USERNAME_SHOPEE:")
print(username_invalidos[['USERNAME_SHOPEE']])

print("\nCarácter especial encontrado en BRAND_NAME_SHOPEE:")
print(brand_invalidos[['BRAND_NAME_SHOPEE']])

# Buscar valores únicos en la columna 'BRAND_NAME_SHOPEE'
nomes_unicos = df_shopee['BRAND_NAME_SHOPEE'].drop_duplicates()

# Mostrar los primeros nombres únicos
print(nomes_unicos)

# Ahora vamos a buscar todos los resultados de todos los meses para crear UNIC_IDs solo para las tiendas nuevas, las que ya existen deben reutilizar sus IDs correspondientes.

# Inicializar df_old vacío para evitar error si falla la query
df_old = pd.DataFrame()

# Conectar a BigQuery
try:
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()
    print("✅ Conexión con BigQuery establecida!")

    query = """
    SELECT *
    FROM `ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS`;
    """

    # Ejecutar la consulta
    response = bigquery_client.query_to_df(query)

    # Acceder al DataFrame
    df_dme = response.df

    print("✅ Acceso a la tabla confirmado!")
    print(df_dme.head())
    print(df_dme.columns)

except BigQueryClientBuilderError as e:
    print(f"Error al construir el cliente: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")

'''
Para cada fila en df_shopee, chequear si hay una tienda igual en df_dme, con el mismo SHOPID y BRAND_NAME_SHOPEE.

Si la encuentra, tomar el UNIC_ID correspondiente de df_dme y ponerlo en la nueva columna UNIC_ID de df_shopee.

Si no la encuentra, generar un nuevo UNIC_ID alfanumérico (5 dígitos, letras mayúsculas) que todavía no exista en df_dme, y agregarlo en df_shopee y también en df_dme, si querés mantener la base actualizada.
'''

# Función para generar nuevo UNIC_ID
def gerar_unic_id_existentes(existentes):
    while True:
        novo_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if novo_id not in existentes:
            return novo_id

# Set con UNIC_IDs que ya existen
unic_ids_existentes = set(df_dme['UNIC_ID'])

# Diccionario para mapear brand_name a UNIC_ID (para que sea consistente)
brand_to_id = dict(zip(df_dme['BRAND_NAME_SHOPEE'], df_dme['UNIC_ID']))

# Lista para guardar nuevas filas que se van a agregar a df_dme
novas_linhas_dme = []

# Lista para llenar la columna en df_shopee
unic_ids_shopee = []

for _, row in df_shopee.iterrows():
    brand = row['BRAND_NAME_SHOPEE']

    # Ya tenemos ese brand registrado?
    if brand in brand_to_id:
        unic_id = brand_to_id[brand]
    else:
        # Brand nuevo: generar nuevo UNIC_ID y registrar
        unic_id = gerar_unic_id_existentes(unic_ids_existentes)
        unic_ids_existentes.add(unic_id)
        brand_to_id[brand] = unic_id  # guardar para próximas veces
        novas_linhas_dme.append({'BRAND_NAME_SHOPEE': brand, 'SHOPID': row['SHOPID'], 'UNIC_ID': unic_id})

    unic_ids_shopee.append(unic_id)

# Agregar columna en df_shopee
df_shopee['UNIC_ID'] = unic_ids_shopee

# Mostrar las nuevas marcas agregadas
novas_lojas_df = pd.DataFrame(novas_linhas_dme)
if not novas_lojas_df.empty:
    print("Nuevas tiendas agregadas con UNIC_IDs nuevos:")
    print(novas_lojas_df)
else:
    print("No se agregaron nuevas tiendas.")

# Actualizar df_dme si querés mantener las nuevas marcas
df_dme = pd.concat([df_dme, novas_lojas_df], ignore_index=True)

print(df_shopee)

# Ver nuevas tiendas agregadas
novas_lojas_df = pd.DataFrame(novas_linhas_dme)

# Imprimir si hay nuevas
if not novas_lojas_df.empty:
    print("Nuevas tiendas agregadas con UNIC_IDs nuevos:")
    print(novas_lojas_df)
else:
    print("No se agregaron nuevas tiendas.")

# Meter los datos del mes en la tabla de backup de datos del scraper

from datetime import datetime as dt

# Crear y llenar columnas extras
horario = dt.now().isoformat()
df_shopee['AUD_INS_DTTM'] = pd.to_datetime(horario)
df_shopee['AUD_UPD_DTTM'] = pd.to_datetime(horario)

# Convertir la columna 'DATE' a tipo datetime
df_shopee['DATE_SCRAPING'] = pd.to_datetime(df_shopee['DATE_SCRAPING'], errors='coerce')

print("Proceso ETL terminado!")

print(df_shopee.dtypes)
print(df_shopee)


# Mandar los datos a la tabla del DME

from melitk.bigquery import BigQueryDatameshClientBuilder, BigQueryClientBuilderError
import pandas as pd
import traceback

try:
    # Construir el cliente de BigQuery
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418")\
        .build()
except Exception as e:
    print("Error completo:")
    traceback.print_exc()
    bigquery_client = None  

if bigquery_client is not None:
    try:
        # Definir el ID de la tabla donde se van a meter los datos
        table_id = 'ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS'
        
        # Configs del job
        job_config_attributes = {
            "create_disposition": "CREATE_NEVER",  # No crear la tabla si no existe
            "mode": "append"  # Meter los datos a la tabla que ya está
        }

        # Meter el DataFrame al BigQuery
        bigquery_client.df_to_gbq(df_shopee, table_id, **job_config_attributes)

        print("Datos metidos con éxito en BigQuery.")
    except Exception as e:
        print(f"Error al meter datos en BigQuery: {e}")
else:
    print("El cliente de BigQuery no se inicializó bien.")

import pandas as pd
from datetime import datetime, datetime as dt
from melitk.bigquery import BigQueryDatameshClientBuilder, BigQueryClientBuilderError

# --------------------------------------------------------
# 1. CONEXIÓN CON BIGQUERY Y EXTRACCIÓN DE LA BASE DEL CRAWLER
# --------------------------------------------------------

# Inicializo df_old vacío solo para evitar errores si la consulta falla
df_old = pd.DataFrame()

try:
    # Construyo el cliente de BigQuery usando la configuración del DME
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()
    
    print("✅ ¡Conexión con BigQuery establecida!")

    # Defino la consulta para obtener los datos de la tabla
    query = """SELECT * FROM `ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS`"""

    # Ejecuto la consulta y guardo el resultado como DataFrame
    response = bigquery_client.query_to_df(query)  
    df_api = response.df  

    print("✅ ¡Acceso a la tabla confirmado!")
    print(df_api.head())  
    print(df_api.columns)

except BigQueryClientBuilderError as e:
    print(f"Error al construir el cliente: {e}")
except Exception as e:
    print(f"Error inesperado: {e}")

# --------------------------------------------------------
# 2. AJUSTES EN FIRST_APPEARENCE (¡HORA DE LIMPIAR 🔍!)
# --------------------------------------------------------

# Me aseguro de que la columna FIRST_APPEARENCE esté en formato datetime
df_api['FIRST_APPEARENCE'] = pd.to_datetime(df_api['FIRST_APPEARENCE'], errors='coerce')

# Creo la máscara para los datos de julio/2025 (ajustar si es necesario en el futuro)
mask_jul25 = (
    (df_api['DATE_SCRAPING'].dt.year == 2025) &
    (df_api['DATE_SCRAPING'].dt.month == 7)
)

# Tomo todo el histórico fuera de julio/2025 que ya tiene FIRST_APPEARENCE lleno
historico = df_api.loc[~mask_jul25 & df_api['FIRST_APPEARENCE'].notna()]

# Creo un diccionario que mapea el menor FIRST_APPEARENCE por UNIC_ID
map_first = (
    historico
        .groupby('UNIC_ID')['FIRST_APPEARENCE']
        .min()
        .to_dict()
)

# Para los registros de julio/2025, intento completar FIRST_APPEARENCE usando el diccionario
df_api.loc[mask_jul25, 'FIRST_APPEARENCE'] = (
    df_api.loc[mask_jul25, 'UNIC_ID'].map(map_first)
)

# Para los que aún quedaron sin fecha (NaT), asigno una fecha por defecto (se puede cambiar)
data_padrao = pd.Timestamp('2025-07-11')
df_api.loc[mask_jul25, 'FIRST_APPEARENCE'] = (
    df_api.loc[mask_jul25, 'FIRST_APPEARENCE']
        .fillna(data_padrao)
)

# Muestro el resultado para confirmar que todo salió bien
print(df_api)

# --------------------------------------------------------
# 3. INSERCIÓN DE LOS DATOS EN BIGQUERY (¡VAMOS CON TODO 🚀!)
# --------------------------------------------------------

try:
    # Reutilizo el mismo cliente (podría recrearse, pero ya está listo aquí)
    bigquery_client = BigQueryDatameshClientBuilder()\
        .with_dme_name("DME_ATTACH_DME000418", env='DEV')\
        .build()

    print("Iniciando la inserción de los datos en BigQuery...")

    # Nombre de la tabla de destino
    table_id = 'ddme000418-dn88p8g386x-furyid.TBL.DM_SHOPEE_OFFICIAL_BRANDS'
    
    # Configuraciones del job de carga
    job_config_attributes = {
        "create_disposition": "CREATE_NEVER",  # No crea una tabla nueva
        "mode": "replace"  # Reemplaza los datos existentes
    }

    # Inserto el DataFrame en BigQuery con las configuraciones definidas
    bigquery_client.df_to_gbq(df_api, table_id, **job_config_attributes)

    print("✅ Datos insertados exitosamente en BigQuery.")
    
except BigQueryClientBuilderError as builder_error:
    print(f"Error al construir el cliente de BigQuery: {builder_error}")
except Exception as e:
    print(f"Ocurrió un error: {e}")
