import pandas as pd
import sqlite3
import shutil
from pathlib import Path

def run_upsert() -> None:
    ROOT = Path(__file__).resolve().parent.parent
    db_path = ROOT / "data" / "yapo.db"
    
    # 1. Cargar y sanitizar datos
    df = pd.read_csv(ROOT / "data" / "processed" / "yapo_clean.csv")
    df = df.where(pd.notnull(df), None)
    
    # 2. Construir arquitectura dinámica de la tabla y UPSERT
    col_defs = ", ".join([f'"{col}" TEXT' if col != "id" else '"id" TEXT PRIMARY KEY' for col in df.columns])
    columnas = ", ".join([f'"{col}"' for col in df.columns])
    placeholders = ", ".join(["?"] * len(df.columns))
    updates = ", ".join([f'"{col}" = excluded."{col}"' for col in df.columns if col != "id"])
    
    sql_upsert = f"""
        INSERT INTO autos ({columnas})
        VALUES ({placeholders})
        ON CONFLICT("id") DO UPDATE SET {updates};
    """
    
    # 3. Ejecución segura y vectorizada
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Crear tabla estrictamente con Primary Key
        cursor.execute(f"CREATE TABLE IF NOT EXISTS autos ({col_defs})")
        
        # Upsert masivo
        cursor.executemany(sql_upsert, df.values.tolist())
        
        # Índices B-Tree para PowerBI
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_marca ON autos("marca")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_region ON autos("region")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precio ON autos("precio_clp")')
        
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