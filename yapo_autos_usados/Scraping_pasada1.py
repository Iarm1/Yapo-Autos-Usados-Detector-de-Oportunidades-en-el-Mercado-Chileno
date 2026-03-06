import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

# ── Configuración ──
API_URL = "https://www.yapo.cl/chile-es/ajax/autos-usados"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

REGIONES = {
    "region-metropolitana": 234,  # ~7.000 anuncios
    "valparaiso":           100,  # ~3.000 anuncios
    "biobio":                84,  # ~2.500 anuncios
    "antofagasta":           50,  # ~1.500 anuncios
    "los-lagos":             34,  # ~1.000 anuncios
}

OUTPUT_PATH = "data/raw/yapo_listing_raw.csv"

# ── Funciones ──
def extraer_titulo(anuncio):
    img = anuncio.find('img', class_='d3-ad-tile__photo')
    if img and img.get('alt'):
        return img['alt']
    titulo_tag = anuncio.find('h2', class_='d3-ad-tile__title')
    if titulo_tag:
        return titulo_tag.get_text(strip=True)
    url_tag = anuncio.find('a', class_='d3-ad-tile__description')
    if url_tag:
        slug = url_tag['href'].split('/')[-2]
        return slug.replace('-', ' ').title()
    return None

def extraer_anuncio(anuncio, region):
    try:
        url_relativa  = anuncio.find('a', class_='d3-ad-tile__description')['href']
        url           = f"https://www.yapo.cl{url_relativa}"
        id_anuncio    = url_relativa.split('/')[-1]
        titulo        = extraer_titulo(anuncio)
        precio_tag    = anuncio.find(class_='d3-ad-tile__price')
        precio_raw    = precio_tag.get_text(strip=True) if precio_tag else None
        vendedor_tag  = anuncio.find('div', class_='d3-ad-tile__seller')
        vendedor      = vendedor_tag.find('span').get_text(strip=True) if vendedor_tag else None
        detalles      = [d.get_text(strip=True) for d in anuncio.find_all('li', class_='d3-ad-tile__details-item')]
        es_destacado  = 'd3-ad-tile--feat-plat' in anuncio.get('class', [])

        return {
            'id':             id_anuncio,
            'titulo':         titulo,
            'precio_clp':     precio_raw,
            'año':            detalles[0] if len(detalles) > 0 else None,
            'combustible':    detalles[1] if len(detalles) > 1 else None,
            'transmision':    detalles[2] if len(detalles) > 2 else None,
            'kilometraje':    detalles[3] if len(detalles) > 3 else None,
            'vendedor':       vendedor,
            'region':         region,
            'destacado':      es_destacado,
            'url':            url,
            'fecha_scraping': datetime.today().strftime('%Y-%m-%d')
        }
    except Exception as e:
        return None

def scrape_pagina(region, pagina):
    params = {
        "sort":       "f_added",
        "dir":        "desc",
        "regionslug": region,
        "page":       pagina,
        "list":       "category"
    }
    try:
        resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"  ✗ Error {resp.status_code} — {region} p.{pagina}")
            return []
        soup     = BeautifulSoup(resp.json()['listing'], 'html.parser')
        anuncios = soup.find_all('div', class_='d3-ad-tile')
        return [r for r in [extraer_anuncio(a, region) for a in anuncios] if r is not None]
    except Exception as e:
        print(f"  ✗ Excepción — {region} p.{pagina}: {e}")
        return []

# ── Pasada 1 completa ──
os.makedirs("data/raw", exist_ok=True)
todos = []
ids_vistos = set()

for region, max_paginas in REGIONES.items():
    print(f"\n── {region} ({max_paginas} páginas) ──")
    for pagina in range(1, max_paginas + 1):
        registros = scrape_pagina(region, pagina)

        # Deduplicar por ID
        nuevos = [r for r in registros if r['id'] not in ids_vistos]
        ids_vistos.update(r['id'] for r in nuevos)
        todos.extend(nuevos)

        if pagina % 10 == 0:
            print(f"  Página {pagina}/{max_paginas} — Total acumulado: {len(todos)}")

        time.sleep(1.5)  # delay entre páginas

    print(f"  ✓ {region} completada")
    time.sleep(5)  # delay extra entre regiones

# ── Guardar ──
df = pd.DataFrame(todos)
df.drop_duplicates(subset='id', inplace=True)
df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
print(f"\n✓ Pasada 1 completada: {len(df)} anuncios → {OUTPUT_PATH}")
