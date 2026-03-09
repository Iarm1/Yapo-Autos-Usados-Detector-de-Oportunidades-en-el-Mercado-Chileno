import pandas as pd
import sqlite3
import shutil
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--full-reload', action='store_true')
args = parser.parse_args()




def run_upsert() -> None:
    ROOT = Path(__file__).resolve().parent.parent
    db_path = ROOT / "data" / "yapo.db"
    # Si se pidió full-reload, borra la DB antes de continuar
    if args.full_reload and db_path.exists():
        db_path.unlink()
        print("🗑️ DB eliminada para recarga completa")


    # 1. Cargar y sanitizar datos
    df = pd.read_csv(ROOT / "data" / "processed" / "yapo_clean.csv")
    df = df.where(pd.notnull(df), None)

    # 2. Mapeo de tipos reales para la tabla
    type_map = {
        "id":               "INTEGER PRIMARY KEY",
        "precio_clp":       "INTEGER",
        "kilometraje":      "INTEGER",
        "año":              "INTEGER",
        "edad_auto":        "INTEGER",
        "destacado":        "INTEGER",
        "fecha_publicacion":"TEXT",
        "fecha_scraping":   "TEXT",
    }
    col_defs     = ", ".join([f'"{col}" {type_map.get(col, "TEXT")}' for col in df.columns])
    columnas     = ", ".join([f'"{col}"' for col in df.columns])
    placeholders = ", ".join(["?"] * len(df.columns))
    updates      = ", ".join([f'"{col}" = excluded."{col}"' for col in df.columns if col != "id"])

    sql_upsert = f"""
        INSERT INTO autos ({columnas})
        VALUES ({placeholders})
        ON CONFLICT("id") DO UPDATE SET {updates};
    """

    # 3. Ejecución segura con manejo correcto de NaN
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS autos ({col_defs})")

        rows = [
            [None if pd.isna(v) else v for v in row]
            for row in df.itertuples(index=False)
        ]
        cursor.executemany(sql_upsert, rows)

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_marca   ON autos("marca")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_region  ON autos("region")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precio  ON autos("precio_clp")')

        total = cursor.execute("SELECT COUNT(*) FROM autos").fetchone()[0]
        print(f"✅ Upsert exitoso. Total de registros en DB: {total:,}")

    # 4. Copia automática a Windows para DBeaver/PowerBI
    windows_path = Path("/mnt/c/Users/Iriqu/Desktop/yapo.db")
    shutil.copy(db_path, windows_path)
    print(f"✅ Copia sincronizada en Windows Desktop: {windows_path}")


if __name__ == "__main__":
    try:
        run_upsert()
    except Exception as e:
        print(f"❌ Error crítico en BD: {e}")
        raise
