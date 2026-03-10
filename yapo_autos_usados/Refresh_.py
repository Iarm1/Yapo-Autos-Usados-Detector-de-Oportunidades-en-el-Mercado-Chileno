import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

LISTING_PATH      = "data/raw/yapo_listing_raw.csv"
DETAIL_PATH       = "data/raw/yapo_detail_raw.csv"
API_URL           = "https://www.yapo.cl/chile-es/ajax/autos-usados"
DELAY             = 2.0
PAGINAS_MAX       = 60
STOP_SI_CONOCIDOS = 15

REGIONES = {
    "region-metropolitana": PAGINAS_MAX,
    "valparaiso":           PAGINAS_MAX,
    "biobio":               PAGINAS_MAX,
    "antofagasta":          PAGINAS_MAX,
    "los-lagos":            PAGINAS_MAX,
}

# ── Función Pasada 1 ──
def scrape_pagina(region, pagina):
    params = {"sort": "f_added", "dir": "desc", "regionslug": region, "page": pagina, "list": "category"}
    try:
        resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=10)
        if resp.status_code != 200:
            return []
        soup     = BeautifulSoup(resp.json()['listing'], 'html.parser')
        anuncios = soup.find_all('div', class_='d3-ad-tile')
        return [r for r in [extraer_anuncio(a, region) for a in anuncios] if r is not None]
    except:
        return []

def extraer_anuncio(anuncio, region):
    try:
        url_relativa = anuncio.find('a', class_='d3-ad-tile__description')['href']
        url          = f"https://www.yapo.cl{url_relativa}"
        id_anuncio   = url_relativa.split('/')[-1]
        img          = anuncio.find('img', class_='d3-ad-tile__photo')
        titulo       = img['alt'] if img and img.get('alt') else None
        precio_tag   = anuncio.find(class_='d3-ad-tile__price')
        precio_raw   = precio_tag.get_text(strip=True) if precio_tag else None
        detalles     = [d.get_text(strip=True) for d in anuncio.find_all('li', class_='d3-ad-tile__details-item')]
        return {
            'id': id_anuncio, 'titulo': titulo, 'precio_clp': precio_raw,
            'año': detalles[0] if len(detalles) > 0 else None,
            'combustible': detalles[1] if len(detalles) > 1 else None,
            'transmision': detalles[2] if len(detalles) > 2 else None,
            'kilometraje': detalles[3] if len(detalles) > 3 else None,
            'region': region, 'url': url,
            'fecha_scraping': datetime.today().strftime('%Y-%m-%d')
        }
    except:
        return None

# ── Función Pasada 2 ──
def extraer_detalle(url, id_anuncio):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {'id': id_anuncio, 'error': f'HTTP {resp.status_code}'}
        soup = BeautifulSoup(resp.text, 'html.parser')

        comuna, fecha_publicacion = None, None
        header_data = soup.find(class_='d3-property-info__header-data')
        if header_data:
            texto             = header_data.get_text(separator='|', strip=True).split('|')
            comuna            = texto[0].strip() if len(texto) > 0 else None
            fecha_publicacion = texto[1].strip() if len(texto) > 1 else None

        marca, modelo = None, None
        for tag in soup.find_all(class_='d3-property-insight__attribute'):
            titulo_tag = tag.find(class_='d3-property-insight__attribute-title')
            valor_tag  = tag.find(class_='d3-property-insight__attribute-value')
            if not titulo_tag or not valor_tag:
                continue
            t = titulo_tag.get_text(strip=True).lower()
            v = valor_tag.get_text(strip=True)
            if 'marca'  in t: marca  = v
            elif 'modelo' in t: modelo = v

        # ── Empresa, dirección y tipo vendedor ──
        empresa, direccion, tipo_vendedor = None, None, None

        vendedor_tag = soup.find(class_='d3-property-contact__address-details')
        if vendedor_tag:
            # Nombre del publicador (siempre presente)
            nombre_tag = vendedor_tag.find(class_='contact_name')
            vendedor_nombre = nombre_tag.get_text(strip=True) if nombre_tag else None

            # Todas las líneas de dirección
            direcciones = [p.get_text(strip=True) 
                        for p in vendedor_tag.find_all(class_='contact_address')]

            if len(direcciones) >= 2:
                # Concesionario: primera línea = empresa, segunda = dirección
                empresa   = direcciones[0]
                direccion = direcciones[1]
                tipo_vendedor = 'profesional'
            elif len(direcciones) == 1:
                # Particular: solo hay ubicación, empresa = nombre del publicador
                empresa   = vendedor_nombre
                direccion = direcciones[0].lstrip('- ').strip()
                tipo_vendedor = 'particular'

            # Confirmar con sello si existe
            sello = vendedor_tag.find('img', title=lambda x: x and x in ['Profesional', 'Particular'])
            if sello:
                tipo_vendedor = sello['title'].lower()


        tipo_vendedor = None
        seal = soup.find('img', alt=lambda x: x and x.strip() in ['Profesional', 'Particular'])
        if seal:
            tipo_vendedor = seal['alt'].lower()

        desc_tag    = soup.find(class_='d3-property-about__text')
        descripcion = desc_tag.get_text(strip=True) if desc_tag else None

        return {
            'id': id_anuncio, 'fecha_publicacion': fecha_publicacion,
            'comuna': comuna, 'marca': marca, 'modelo': modelo,
            'empresa': empresa, 'direccion': direccion,
            'tipo_vendedor': tipo_vendedor, 'descripcion': descripcion,
            'error': None
        }
    except Exception as e:
        return {'id': id_anuncio, 'error': str(e)}


# ══════════════════════════════════════
# PASO 1: Refresh Pasada 1
# ══════════════════════════════════════
df_listing    = pd.read_csv(LISTING_PATH)
ids_conocidos = set(df_listing['id'].astype(str))
print(f"Anuncios existentes: {len(df_listing)}")

nuevos_listing = []

for region, max_pag in REGIONES.items():
    print(f"\n── {region} ──")
    conocidos_seguidos = 0

    for pagina in range(1, max_pag + 1):
        registros = scrape_pagina(region, pagina)

        for r in registros:
            if r.get('destacado'):
                continue
            if r['id'] in ids_conocidos:
                conocidos_seguidos += 1
            else:
                nuevos_listing.append(r)
                ids_conocidos.add(r['id'])
                conocidos_seguidos = 0

        print(f"  Página {pagina} — nuevos: {len(nuevos_listing)}")

        if conocidos_seguidos >= STOP_SI_CONOCIDOS:
            print(f"  ✓ Stop: {conocidos_seguidos} IDs conocidos seguidos")
            break

        time.sleep(1.5)

df_listing_updated = pd.concat([df_listing, pd.DataFrame(nuevos_listing)], ignore_index=True)
df_listing_updated.drop_duplicates(subset='id', inplace=True)
df_listing_updated.to_csv(LISTING_PATH, index=False, encoding='utf-8-sig')
print(f"\n✓ Pasada 1 refresh: +{len(nuevos_listing)} nuevos → total {len(df_listing_updated)}")

# ══════════════════════════════════════
# PASO 2: Detalle de anuncios nuevos
# ══════════════════════════════════════
if len(nuevos_listing) == 0:
    print("\n✓ Sin anuncios nuevos")
else:
    print(f"\n── Pasada 2 para {len(nuevos_listing)} anuncios nuevos ──")
    df_detail       = pd.read_csv(DETAIL_PATH) if os.path.exists(DETAIL_PATH) else pd.DataFrame()
    ids_con_detalle = set(df_detail['id'].astype(str)) if len(df_detail) > 0 else set()
    nuevos_detail   = []

    for i, r in enumerate(nuevos_listing, 1):
        if str(r['id']) in ids_con_detalle:
            continue
        resultado = extraer_detalle(r['url'], str(r['id']))
        nuevos_detail.append(resultado)
        if i % 10 == 0:
            print(f"  [{i}/{len(nuevos_listing)}] {resultado.get('marca')} {resultado.get('modelo')} | {resultado.get('fecha_publicacion')}")
        time.sleep(DELAY)

    df_detail_updated = pd.concat([df_detail, pd.DataFrame(nuevos_detail)], ignore_index=True)
    df_detail_updated.drop_duplicates(subset='id', inplace=True)
    df_detail_updated.to_csv(DETAIL_PATH, index=False, encoding='utf-8-sig')
    print(f"\n✓ Pasada 2 refresh: +{len(nuevos_detail)} detalles → total {len(df_detail_updated)}")

print(f"\n✓ Refresh completo: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
