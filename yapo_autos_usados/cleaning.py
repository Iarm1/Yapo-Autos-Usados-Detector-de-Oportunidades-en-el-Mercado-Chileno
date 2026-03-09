import pandas as pd
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
df = pd.read_csv(ROOT / "data/raw/yapo_full_raw.csv")
print(f"Registros iniciales: {len(df)}")

# ── 1. Eliminar columnas inútiles ──────────────────────────
df.drop(columns=['error', 'tipo_vendedor', 'direccion'], inplace=True)

# ── 2. Transformaciones numéricas ─────────────────────────
def limpiar_precio(serie):
    return (serie
        .astype(str)
        .str.strip()
        .str.replace(r"\s*-[\d,\.]+%.*$", "", regex=True)  # cubre espacios y decimales en %
            # corta -7%, -13%, etc.
        .str.replace(",", "", regex=False)           # elimina comas como separador
        .str.replace(r"[^\d]", "", regex=True)       # deja solo dígitos
        .pipe(pd.to_numeric, errors='coerce'))

df['precio_clp'] = limpiar_precio(df['precio_clp'])


def limpiar_km(serie):
    return (serie
        .astype(str)
        .str.replace(r"['\.,\skm]", "", regex=True, flags=re.IGNORECASE)
        .pipe(pd.to_numeric, errors='coerce'))

df['kilometraje'] = limpiar_km(df['kilometraje'])


df['año'] = pd.to_numeric(df['año'], errors='coerce')

# ── 3. Fechas ─────────────────────────────────────────────
df['fecha_publicacion'] = pd.to_datetime(
    df['fecha_publicacion'], format='%d/%m/%Y', errors='coerce')
df['fecha_scraping'] = pd.to_datetime(
    df['fecha_scraping'], errors='coerce')

# ── 4. Texto ──────────────────────────────────────────────
df['marca']   = df['marca'].str.strip().str.title()
df['modelo']  = df['modelo'].str.strip().str.title()
df['comuna']  = df['comuna'].str.strip().str.title()
df['empresa'] = df['empresa'].fillna('Particular')
df['descripcion'] = df['descripcion'].fillna('')

# ── 5. Feature engineering ────────────────────────────────
df['edad_auto'] = 2026 - df['año']

# ── 6. Imputar kilometraje con mediana por marca+año ──────
df['kilometraje'] = df.groupby(['marca', 'año'])['kilometraje'] \
    .transform(lambda x: x.fillna(x.median()))
df['kilometraje'] = df['kilometraje'].fillna(df['kilometraje'].median())

# ── 7. Drop filas sin target o sin marca ──────────────────
antes = len(df)
df = df.dropna(subset=['precio_clp', 'marca'])
print(f"Eliminados sin precio/marca: {antes - len(df)}")

# ── 8. Filtros de outliers ────────────────────────────────
antes = len(df)
df = df[df['precio_clp'].between(1_000_000, 250_000_000)]
print(f"Eliminados por outlier precio:      {antes - len(df)}")

antes = len(df)
df = df[df['kilometraje'].between(0, 500_000)]
print(f"Eliminados por outlier kilometraje: {antes - len(df)}")

antes = len(df)
df = df[df['año'].between(1990, 2026)]
print(f"Eliminados por año fuera de rango:  {antes - len(df)}")

# 1. Normalizar transmisión
df['transmision'] = df['transmision'].replace({'5+': 'Otros'})

# 2. Normalizar región
region_map = {
    'region-metropolitana': 'Metropolitana',
    'valparaiso':           'Valparaíso',
    'biobio':               'Biobío',
    'antofagasta':          'Antofagasta',
    'los-lagos':            'Los Lagos'
}
df['region'] = df['region'].map(region_map).fillna(df['region'])

# 3. Agrupar marcas pequeñas
top_marcas = df['marca'].value_counts()
marcas_validas = top_marcas[top_marcas >= 30].index
df['marca'] = df['marca'].where(df['marca'].isin(marcas_validas), other='Otra')

# Convertir a entero nullable (soporta NaN sin convertir a float)
df['precio_clp']  = df['precio_clp'].round(0).astype('Int64')
df['kilometraje'] = df['kilometraje'].round(0).astype('Int64')
df['año']         = df['año'].round(0).astype('Int64')
df['edad_auto']   = df['edad_auto'].round(0).astype('Int64')

# ── 9. Guardar ────────────────────────────────────────────
print(f"\nRegistros finales: {len(df)}")
print(df.dtypes)
df.to_csv(ROOT / "data/processed/yapo_clean.csv", index=False)
print("\n✅ Guardado en data/processed/yapo_clean.csv")
