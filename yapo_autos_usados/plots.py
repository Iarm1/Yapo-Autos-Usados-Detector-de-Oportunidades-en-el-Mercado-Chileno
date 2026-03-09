import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from loguru import logger
import typer

app = typer.Typer()

@app.command()
def generar_graficos() -> None:
    ROOT = Path(__file__).resolve().parents[1]
    db_path = ROOT / "data" / "yapo.db"
    output_dir = ROOT / "reports" / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Conectando a SQLite y extrayendo datos limpios...")
    with sqlite3.connect(db_path) as conn:
        # CORRECCIÓN 1: CAST de SQL para traer la variable como número (Float/Real)
        query = """
            SELECT marca, año, CAST(precio_clp AS REAL) as precio_clp
            FROM autos 
            WHERE precio_clp IS NOT NULL AND año >= 2010
        """
        df = pd.read_sql(query, conn)

    logger.info("Calculando Top 5 marcas y generando Boxplot...")
    top_marcas = df['marca'].value_counts().nlargest(5).index
    df_top = df[df['marca'].isin(top_marcas)]

    plt.figure(figsize=(10, 6))
    sns.set_theme(style="whitegrid")
    
    # CORRECCIÓN 2: Agregar hue='marca' y legend=False por exigencia de nuevas versiones de Seaborn
    sns.boxplot(
        data=df_top, 
        x='marca', 
        y='precio_clp', 
        hue='marca', 
        palette="Set2", 
        legend=False
    )
    
    plt.title('Distribución de Precios por Top 5 Marcas (2010-2026)', fontsize=14)
    plt.ylabel('Precio (CLP)', fontsize=12)
    plt.xlabel('Marca', fontsize=12)
    
    # Ahora sí funcionará porque y es un número real
    plt.ticklabel_format(style='plain', axis='y')

    out_file = output_dir / "precio_vs_marca.png"
    plt.savefig(out_file, bbox_inches='tight', dpi=300)
    logger.success(f"Gráfico exportado exitosamente en: {out_file}")

if __name__ == "__main__":
    app()