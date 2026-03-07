import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
df = pd.read_csv(ROOT / "data/raw/yapo_full_raw.csv")
print(f"Registros iniciales: {len(df)}")

# ── 1. Eliminar columnas inútiles ──────────────────────────
df.drop(columns=['error', 'tipo_vendedor', 'direccion'], inplace=True)

# ── 2. Transformaciones numéricas ─────────────────────────
df['precio_clp'] = (df['precio_clp']
    .astype(str)
    .str.replace(r"[^\d]", "", regex=True)
    .pipe(pd.to_numeric, errors='coerce'))

df['kilometraje'] = (df['kilometraje']
    .astype(str)
    .str.replace(r"[^\d]", "", regex=True)
    .pipe(pd.to_numeric, errors='coerce'))

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

# ── 9. Guardar ────────────────────────────────────────────
print(f"\nRegistros finales: {len(df)}")
print(df.dtypes)
df.to_csv(ROOT / "data/processed/yapo_clean.csv", index=False)
print("\n✅ Guardado en data/processed/yapo_clean.csv")
