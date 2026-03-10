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

        # Agrega esto ANTES del bloque de fecha/comuna
        # ── Detectar anuncio expirado ──
        # ── Detectar anuncio expirado ──
        expirado_tag = soup.find(class_='exp')
        if expirado_tag:
            return {
                'id':                id_anuncio,
                'fecha_publicacion': None,
                'comuna':            None,
                'marca':             None,
                'modelo':            None,
                'empresa':           None,
                'direccion':         None,
                'tipo_vendedor':     'expirado',
                'descripcion':       None,
                'error':             'anuncio_expirado'
            }



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
            elif len(direcciones) == 0:
                empresa = vendedor_nombre
                direccion = None
                tipo_vendedor = 'particular'

            # Confirmar con sello si existe
            sello = vendedor_tag.find('img', title=lambda x: x and x in ['Profesional', 'Particular'])
            if sello:
                tipo_vendedor = sello['title'].lower()


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
            'tipo_vendedor': tipo_vendedor if tipo_vendedor else 'sin_info',

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
