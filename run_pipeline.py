import subprocess
import sys

def run_script(script_path):
    print(f"🚀 Iniciando: {script_path}...")
    # Ejecuta el script y captura la salida
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    # Imprimir lo que el script "dijo" en consola
    print(result.stdout)
    
    if result.returncode != 0:
        print(f"❌ Error crítico en {script_path}:\n{result.stderr}")
        sys.exit(1) # Detiene todo el pipeline si algo falla
        
    print(f"✅ {script_path} finalizado correctamente.\n")
    print("-" * 50)

if __name__ == "__main__":
    print("🌟 INICIANDO PIPELINE DE YAPO AUTOS 🌟\n")
    
    # Lista de tus scripts en el orden exacto de ejecución
    scripts = [
        "yapo_autos_usados/Refresh_.py",
        "yapo_autos_usados/join_csv.py",
        "yapo_autos_usados/cleaning.py",
        "yapo_autos_usados/Load_sql_GCS.py" # Tu nuevo archivo
    ]
    
    for script in scripts:
        run_script(script)
        
    print("🎉 PIPELINE COMPLETADO CON ÉXITO. DATOS EN BIGQUERY.")