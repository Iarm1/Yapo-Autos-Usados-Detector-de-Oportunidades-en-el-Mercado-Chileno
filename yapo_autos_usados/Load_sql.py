import pandas as pd
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

df = pd.read_csv(ROOT / "data/processed/yapo_clean.csv")

conn = sqlite3.connect(ROOT / "data/yapo.db")

df.to_sql("autos", conn, if_exists="replace", index=False)

# Crear índices para consultas rápidas en PowerBI
conn.execute("CREATE INDEX IF NOT EXISTS idx_marca   ON autos(marca)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_region  ON autos(region)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_precio  ON autos(precio_clp)")
conn.commit()

# Verificación
total = conn.execute("SELECT COUNT(*) FROM autos").fetchone()[0]
print(f"✅ Tabla 'autos' creada con {total:,} registros en data/yapo.db")

# Vista previa
print(pd.read_sql("SELECT marca, modelo, año, precio_clp, region FROM autos LIMIT 5", conn))
conn.close()
conn = sqlite3.connect(ROOT / "data/yapo.db", timeout=30)
import shutil

# Copia automática a Windows para DBeaver
windows_path = Path("/mnt/c/Users/Iriqu/Desktop/yapo.db")
shutil.copy(ROOT / "data/yapo.db", windows_path)
print("✅ Copia sincronizada en Windows Desktop")
