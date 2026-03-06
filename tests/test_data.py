import requests
from bs4 import BeautifulSoup
import json

url = "https://www.yapo.cl/chile-es/ajax/autos-usados"

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

params = {
    "sort": "f_added",
    "dir": "desc",
    "regionslug": "region-metropolitana",
    "page": 1,
    "list": "category"
}

response = requests.get(url, headers=headers, params=params)
print(response.status_code)
print(response.json().keys())

data = response.json()
soup = BeautifulSoup(data['listing'], 'html.parser')
# Ver las primeras etiquetas disponibles en el listing
print(soup.prettify()[:5000])
# Buscar todas las etiquetas disponibles en el nivel superior
#tags = {}
#for tag in soup.find_all(True):
    #tags[tag.name] = tags.get(tag.name, 0) + 1

# Ver qué etiquetas hay y cuántas veces aparecen
#print(sorted(tags.items(), key=lambda x: x[1], reverse=True)[:20])
# Ver el primer li con su clase
#primer_li = soup.find('li')
#print(primer_li.get('class'))
#print(primer_li.prettify()[:3000])

# Ver la estructura del primer anuncio
#primer_anuncio = soup.find('article')
#print(primer_anuncio.prettify()[:3000])
# Buscar todas las tarjetas de anuncios
anuncios = soup.find_all('div', class_='d3-ad-tile')
print(f"Anuncios encontrados: {len(anuncios)}")

# Ver estructura completa del primer anuncio
print(anuncios[0].prettify()[:3000])
