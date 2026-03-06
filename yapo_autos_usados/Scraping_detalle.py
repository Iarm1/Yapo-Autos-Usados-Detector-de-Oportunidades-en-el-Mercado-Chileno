import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

INPUT_PATH  = "data/raw/yapo_listing_raw.csv"
OUTPUT_PATH = "data/raw/yapo_detail_raw.csv"
CHECKPOINT  = "data/raw/yapo_detail_checkpoint.csv"
DELAY       = 2.0
BATCH_SIZE  = 200

def extraer_detalle(url, id_anuncio):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {'id': id_anuncio, 'error': f'HTTP {resp.status_code}'}

        soup = BeautifulSoup(resp.text, 'html.parser')

        # ── Fecha y comuna ──
        comuna, fecha_publicacion = None, None
        header_data = soup.find(class_='d3-property-info__header-data')
        if header_data:
            texto = header_data.get_text(separator='|', strip=True).split('|')
            comuna             = texto[0].strip() if len(texto) > 0 else None
            fecha_publicacion  = texto[1].strip() if len(texto) > 1 else None

        # ── Marca y modelo desde insights ──
        marca, modelo = None, None
        for tag in soup.find_all(class_='d3-property-insight__attribute'):
            titulo_tag = tag.find(class_='d3-property-insight__attribute-title')
            valor_tag  = tag.find(class_='d3-property-insight__attribute-value')
            if not titulo_tag or not valor_tag:
                continue
            titulo_txt = titulo_tag.get_text(strip=True).lower()
            valor_txt  = valor_tag.get_text(strip=True)
            if 'marca'  in titulo_txt: marca  = valor_txt
            elif 'modelo' in titulo_txt: modelo = valor_txt

        # ── Empresa y dirección del vendedor ──
        empresa, direccion = None, None
        vendedor_tag = soup.find(class_='d3-property-contact__address-details')
        if vendedor_tag:
            textos    = [t.strip() for t in vendedor_tag.get_text(separator='|').split('|') if t.strip()]
            empresa   = textos[1] if len(textos) > 1 else None
            direccion = textos[2] if len(textos) > 2 else None

        # ── Tipo de vendedor ──
        tipo_vendedor = None
        seal = soup.find('img', alt=lambda x: x and x.strip() in ['Profesional', 'Particular'])
        if seal:
            tipo_vendedor = seal['alt'].lower()

        # ── Descripción ──
        desc_tag    = soup.find(class_='d3-property-about__text')
        descripcion = desc_tag.get_text(strip=True) if desc_tag else None

        return {
            'id':                id_anuncio,
            'fecha_publicacion': fecha_publicacion,
            'comuna':            comuna,
            'marca':             marca,
            'modelo':            modelo,
            'empresa':           empresa,
            'direccion':         direccion,
            'tipo_vendedor':     tipo_vendedor,
            'descripcion':       descripcion,
            'error':             None
        }

    except Exception as e:
        return {'id': id_anuncio, 'error': str(e)}


# ── Cargar Pasada 1 ──
df_listing = pd.read_csv(INPUT_PATH)
print(f"Total anuncios Pasada 1: {len(df_listing)}")

# ── Retomar desde checkpoint si existe ──
if os.path.exists(CHECKPOINT):
    df_done  = pd.read_csv(CHECKPOINT)
    ids_done = set(df_done['id'].astype(str))
    print(f"Retomando desde checkpoint: {len(ids_done)} ya procesados")
else:
    df_done  = pd.DataFrame()
    ids_done = set()

pendientes = df_listing[~df_listing['id'].astype(str).isin(ids_done)]
print(f"Pendientes: {len(pendientes)}")

# ── Loop principal ──
resultados = []
total = len(pendientes)

for i, (_, row) in enumerate(pendientes.iterrows(), 1):
    resultado = extraer_detalle(row['url'], str(row['id']))
    resultados.append(resultado)

    if i % 50 == 0:
        pct = round(i / total * 100, 1)
        print(f"  [{i}/{total}] {pct}% — {resultado.get('marca')} {resultado.get('modelo')} | {resultado.get('fecha_publicacion')}")

    # Checkpoint cada 200
    if i % BATCH_SIZE == 0:
        df_batch      = pd.DataFrame(resultados)
        df_checkpoint = pd.concat([df_done, df_batch], ignore_index=True)
        df_checkpoint.to_csv(CHECKPOINT, index=False, encoding='utf-8-sig')
        print(f"  ✓ Checkpoint guardado ({len(df_checkpoint)} registros)")

    time.sleep(DELAY)

# ── Guardar final ──
df_final = pd.concat([df_done, pd.DataFrame(resultados)], ignore_index=True)
df_final.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
print(f"\n✓ Pasada 2 completa: {len(df_final)} registros → {OUTPUT_PATH}")
