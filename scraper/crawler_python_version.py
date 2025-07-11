import requests
import pandas as pd
from datetime import datetime

# Configuraciones de la URL y headers para hacer la solicitud
url = "https://shopee.com.br/api/v4/official_shop/get_shops_by_category"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "x-api-source": "pc",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://shopee.com.br/oficial/brands"
}

# Lista de categorías (¡esto es súper importante! siempre mantenlo actualizado si cambias algo)
categories = {
    -1: "Página Principal",
    11059998: "Ropa Femenina",
    11059983: "Hogar y Construcción",
    11059974: "Belleza",
    11059986: "Ropa Masculina",
    11059999: "Zapatos Femeninos",
    11059987: "Zapatos Masculinos",
    11059988: "Celulares y Dispositivos",
    11059973: "Moda Infantil",
    11059978: "Accesorios de Moda",
    11059992: "Deportes y Ocio",
    11059984: "Electrodomésticos pequeños",
    11059982: "Juguetes y Hobbies",
    11059972: "Automóviles",
    11059981: "Salud",
    11059989: "Maternidad y Bebés",
    11059971: "Audio",
    11059993: "Papelería",
    11059997: "Bolsos Femeninos",
    11059985: "Bolsos Masculinos",
    11059991: "Mascotas",
    11059990: "Motocicletas",
    11059977: "Computadoras y Accesorios",
    11059979: "Comida y Bebida",
    11059980: "Juegos y Consolas",
    11059975: "Libros y Revistas"
}

# Lista donde vamos a guardar toda la info de las marcas
all_brands_data = []

# Recorriendo todas las categorías
for category_id, category_name in categories.items():
    # Parámetros específicos para cada categoría
    params = {
        "need_zhuyin": 0,
        "category_id": category_id
    }

    # Haciendo la solicitud HTTP
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        print(f"¡Solicitud exitosa para la categoría {category_name}!")
    else:
        print(f"Error en la solicitud para la categoría {category_name}. Código de estado: {response.status_code}")
        continue  # Saltamos esta categoría si algo salió mal

    # Parseando la respuesta como JSON
    response_json = response.json()

    # Checando si la estructura que esperamos está en la respuesta
    if 'data' in response_json and 'brands' in response_json['data']:
        # Recorriendo las marcas dentro de la respuesta
        for brand in response_json['data']['brands']:
            for brand_id in brand['brand_ids']:
                # Creamos la URL pública del shop
                url_to = f"https://shopee.com.br/{brand_id['shopid']}"
                # Añadimos todo a la lista
                all_brands_data.append({
                    'index': brand['index'],
                    'total': brand['total'],
                    'username': brand_id['username'],
                    'brand_name': brand_id['brand_name'],
                    'shopid': brand_id['shopid'],
                    'logo': brand_id['logo'],
                    'logo_pc': brand_id['logo_pc'],
                    'shop_collection_id': brand_id['shop_collection_id'],
                    'ctime': brand_id['ctime'],
                    'brand_label': brand_id['brand_label'],
                    'shop_type': brand_id['shop_type'],
                    'redirect_url': brand_id['redirect_url'],
                    'entity_id': brand_id['entity_id'],
                    'category_id': category_id,
                    'category_name': category_name,
                    'url_to': url_to,
                    'data_requisicao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
    else:
        print(f"La estructura esperada no está presente en la respuesta para la categoría {category_name}.")

# Convertimos la lista a un DataFrame de pandas
df = pd.DataFrame(all_brands_data)

# Generamos la fecha actual en formato YYYYMMDD
data_atual = datetime.now().strftime('%Y%m%d')

# Guardamos el archivo CSV con la fecha en el nombre
nome_arquivo = f"brands_shopee_{data_atual}.csv"
df.to_csv(nome_arquivo, index=False)

print(f"Datos guardados en {nome_arquivo}")
