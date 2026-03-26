import pandas as pd
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

df = pd.read_csv(ROOT / "data/processed/yapo_clean.csv")

# ── BLOQUE 1: Variable de tratamiento D ──────────────────────────────
# Una automotora es cualquier vendedor con razón social distinta a 'Particular'
df['es_automotora'] = (df['tipo_vendedor'].str.strip().str.lower() != 'particular').astype(int)

print(df['es_automotora'].value_counts())
# Verificación de overlap (requisito del DAG)
print(df.groupby('es_automotora')['precio_clp'].describe())
