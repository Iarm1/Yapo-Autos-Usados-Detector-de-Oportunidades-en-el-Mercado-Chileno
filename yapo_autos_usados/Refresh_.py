import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

# Reutiliza las mismas funciones de dataset.py
from yapo_autos_usados.dataset import scrape_pagina

INPUT_PATH  = "data/raw/yapo_listing_raw.csv"
OUTPUT_PATH = "data/raw/yapo_listing_raw.csv"  # sobreescribe el mismo archivo
PAGINAS_MAX = 1   # cuántas páginas revisar por región
STOP_SI_CONOCIDOS = 20  # para si encuentra 20 IDs ya conocidos seguidos

REGIONES = {
    "region-metropolitana": PAGINAS_MAX,
    "valparaiso":           PAGINAS_MAX,
    "biobio":               PAGINAS_MAX,
    "antofagasta":          PAGINAS_MAX,
    "los-lagos":            PAGINAS_MAX,
}

# ── Cargar existentes ──
df_existente = pd.read_csv(INPUT_PATH)
ids_conocidos = set(df_existente['id'].astype(str))
print(f"Anuncios existentes: {len(df_existente)}")

nuevos = []

for region, max_pag in REGIONES.items():
    print(f"\n── {region} ──")
    conocidos_seguidos = 0

    for pagina in range(1, max_pag + 1):
        registros = scrape_pagina(region, pagina)

        for r in registros:
            if r['id'] in ids_conocidos:
                conocidos_seguidos += 1
            else:
                nuevos.append(r)
                ids_conocidos.add(r['id'])
                conocidos_seguidos = 0  # reset

        print(f"  Página {pagina} — nuevos encontrados: {len(nuevos)}")

        # Si hay muchos conocidos seguidos, ya alcanzamos los viejos
        if conocidos_seguidos >= STOP_SI_CONOCIDOS:
            print(f"  ✓ Stop: {conocidos_seguidos} IDs conocidos seguidos")
            break

        time.sleep(1.5)

# ── Guardar combinado ──
df_nuevos = pd.DataFrame(nuevos)
df_combinado = pd.concat([df_existente, df_nuevos], ignore_index=True)
df_combinado.drop_duplicates(subset='id', inplace=True)
df_combinado.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
print(f"\n✓ Refresh completo: +{len(df_nuevos)} nuevos → total {len(df_combinado)}")
