import pandas as pd
from google.cloud import bigquery
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--full-reload', action='store_true')
args = parser.parse_args()

def run_bigquery_upsert():
    # 1. Configuración de nombres (¡Reemplaza con tus datos!)
    BUCKET_NAME = "yapo-autos-datalake"
    PROJECT_ID = "portafolio-yapo-autos"  # Ej: portafolio-yapo-autos-12345
    DATASET_ID = "yapo_dataset"
    TABLE_NAME = "autos"
    
    # Rutas
    INPUT_PATH = f"gs://{BUCKET_NAME}/processed/yapo_clean.csv"
    FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    TEMP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}_temp"

    # 2. Leer los datos limpios desde tu Data Lake
    print(f"📥 Leyendo datos desde {INPUT_PATH}...")
    df = pd.read_csv(INPUT_PATH)

    # 3. Inicializar el cliente de BigQuery
    client = bigquery.Client(project=PROJECT_ID)

    # Si es full reload, borramos la tabla principal para empezar de cero
    if args.full_reload:
        client.delete_table(FINAL_TABLE, not_found_ok=True)
        print("🗑️ Tabla final eliminada para recarga completa.")

    # 4. Subir el DataFrame a una tabla temporal en BigQuery
    print(f"📤 Subiendo {len(df)} registros a tabla temporal...")
    df.to_gbq(destination_table=TEMP_TABLE, project_id=PROJECT_ID, if_exists='replace')

    # 5. Ejecutar el MERGE (Upsert)
    # Compara la tabla temporal con la final usando el "id"
    merge_query = f"""
        MERGE `{FINAL_TABLE}` T
        USING `{TEMP_TABLE}` S
        ON T.id = S.id
        WHEN MATCHED THEN
            UPDATE SET 
                precio_clp = S.precio_clp,
                kilometraje = S.kilometraje,
                destacado = S.destacado
                -- (Puedes agregar más campos aquí si suelen cambiar, o quitar el UPDATE si solo quieres insertar nuevos)
        WHEN NOT MATCHED THEN
            INSERT ROW
    """
    
    print("🔄 Ejecutando Upsert en BigQuery...")
    # Si la tabla final no existe (primera vez), BigQuery lanzará error en el MERGE, 
    # así que la creamos copiando la temporal.
    try:
        client.query(merge_query).result()
    except Exception as e:
        if "was not found" in str(e):
            print("✨ Tabla principal no existe. Creándola por primera vez...")
            client.query(f"CREATE TABLE `{FINAL_TABLE}` AS SELECT * FROM `{TEMP_TABLE}`").result()
        else:
            raise e

    # 6. Limpieza: Borrar tabla temporal
    client.delete_table(TEMP_TABLE, not_found_ok=True)
    
    # 7. Contar total final para validar
    query_job = client.query(f"SELECT COUNT(*) as total FROM `{FINAL_TABLE}`")
    total = list(query_job.result())[0].total
    print(f"✅ Upsert exitoso. Total de registros en BigQuery: {total:,}")

if __name__ == "__main__":
    try:
        run_bigquery_upsert()
    except Exception as e:
        print(f"❌ Error crítico en BD: {e}")
        raise