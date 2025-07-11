// Etapa #00

const axios = require('axios'); // Usando axios para hacer las peticiones HTTP
const fs = require('fs'); // Módulo nativo para manejar archivos, aunque acá ni lo terminé usando directamente
const dayjs = require('dayjs'); // Librería para manejar fechas de forma sencilla
const createCsvWriter = require('csv-writer').createObjectCsvWriter; // Usé esto para escribir los datos en un CSV, pero se puede adaptar para mandar directo a BigQuery

// URL de la API de Shopee que lista las tiendas oficiales por categoría
const url = 'https://shopee.com.br/api/v4/official_shop/get_shops_by_category';

// Headers que imitan un navegador real para evitar bloqueos o respuestas incompletas de Shopee
const headers = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Accept': '*/*',
  'x-api-source': 'pc',
  'x-requested-with': 'XMLHttpRequest',
  'referer': 'https://shopee.com.br/oficial/brands'
};

// Diccionario con los IDs de categorías y sus nombres (esto ayuda a visualizar y también es necesario para loguear)
const categories = {
  "-1": "Página Principal", // Hay que traer y manejar esta también, porque hay tiendas nuevas que todavía no tienen categoría asignada
  11059998: "Roupas Femininas",
  11059983: "Casa e Construção",
  11059974: "Beleza",
  11059986: "Roupas Masculinas",
  11059999: "Sapatos Femininos",
  11059987: "Sapatos Masculinos",
  11059988: "Celulares e Dispositivos",
  11059973: "Moda Infantil",
  11059978: "Acessórios de Moda",
  11059992: "Esportes e Lazer",
  11059984: "Eletroportáteis",
  11059982: "Brinquedos e Hobbies",
  11059972: "Automóveis",
  11059981: "Saúde",
  11059989: "Mãe e Bebê",
  11059971: "Áudio",
  11059993: "Papelaria",
  11059997: "Bolsas Femininas",
  11059985: "Bolsas Masculinas",
  11059991: "Animais Domésticos",
  11059990: "Motocicletas",
  11059977: "Computadores e Acessórios",
  11059979: "Alimentos e Bebidas",
  11059980: "Jogos e Consoles",
  11059975: "Livros e Revistas"
};

// Acá creé una lista para ir juntando todos los datos de las marcas por categoría. Después hay que sacar los duplicados de la página principal ;)
const allBrandsData = [];

(async () => {
  // Loop por las categorías para ir haciendo las peticiones una a una
  for (const [categoryId, categoryName] of Object.entries(categories)) {
    try {
      // Hago la petición con axios, pasando los headers y los parámetros correctos
      const response = await axios.get(url, {
        headers,
        params: {
          need_zhuyin: 0, // parámetro obligatorio de la API, aunque no me sirva para nada
          category_id: categoryId
        }
      });

      console.log(`¡Petición exitosa para la categoría ${categoryName}!`);

      const data = response.data;

      // Reviso si vino el array de marcas como esperaba
      if (data?.data?.brands) {
        for (const brand of data.data.brands) {
          for (const brandId of brand.brand_ids) {
            // Acá armo el objeto con todos los campos que me interesan
            allBrandsData.push({
              index: brand.index,
              total: brand.total,
              username: brandId.username,
              brand_name: brandId.brand_name,
              shopid: brandId.shopid,
              logo: brandId.logo,
              logo_pc: brandId.logo_pc,
              shop_collection_id: brandId.shop_collection_id,
              ctime: brandId.ctime,
              brand_label: brandId.brand_label,
              shop_type: brandId.shop_type,
              redirect_url: brandId.redirect_url,
              entity_id: brandId.entity_id,
              category_id: categoryId,
              category_name: categoryName,
              url_to: `https://shopee.com.br/${brandId.shopid}`, // URL directa para la tienda
              data_requisicao: dayjs().format('YYYY-MM-DD HH:mm:ss') // Registro de la fecha y hora de la recolección
            });
          }
        }
      } else {
        console.log(`La estructura esperada no está presente en la respuesta de la categoría ${categoryName}.`);
      }
    } catch (error) {
      // Captura y muestra el error con el status de la respuesta (si viene)
      console.error(`Error en la petición para la categoría ${categoryName}:`, error.response?.status || error.message);
    }
  }

  // Acá empieza la parte de guardar el CSV con los datos recolectados
  const dataAtual = dayjs().format('YYYYMMDD');
  const nomeArquivo = `brands_shopee_${dataAtual}.csv`; // Nombre del archivo con la fecha del día

  const csvWriter = createCsvWriter({
    path: nomeArquivo,
    header: Object.keys(allBrandsData[0] || {}).map(key => ({ id: key, title: key })), // Encabezado basado en las claves del primer ítem
    fieldDelimiter: ',' // Delimitador por defecto
  });

  // Escribiendo todos los datos en el CSV
  await csvWriter.writeRecords(allBrandsData);
  console.log(`Datos guardados en ${nomeArquivo}`);
})();
