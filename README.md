# Autos_usados

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

Analisis de venta de autos usados Chile

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         yapo_autos_usados and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
├── setup.cfg          <- Configuration file for flake8
│
└── yapo_autos_usados   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes yapo_autos_usados a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    ├── modeling                
    │   ├── __init__.py 
    │   ├── predict.py          <- Code to run model inference with trained models          
    │   └── train.py            <- Code to train models
    │
    └── plots.py                <- Code to create visualizations
```

--------

Actualizacion diaria para datos nuevos en local
REFRESH_.py --> join_csv.py --> cleaning.py --> Load_sql.py(en caso de querer recargar la base de datos en sql usar --full-reload en bash)

# Detector de Oportunidades en Yapo Autos (Mercado Chileno)

## Resumen del Proyecto
Este proyecto automatiza la extracción, limpieza y almacenamiento de anuncios de autos usados en Chile. Su objetivo es detectar oportunidades de mercado (autos subvalorados) alimentando un dashboard analítico.

## Arquitectura de Datos (Modern Data Stack)
El proyecto pasó de una ejecución local a una arquitectura Cloud 100% serverless en Google Cloud Platform (GCP):

1. **Extracción y Limpieza (Python + Pandas):** Scripts que extraen datos usando BeautifulSoup y procesan DataFrames.
2. **Orquestación (Cloud Scheduler):** Gatilla el proceso todos los días a las 8:00 AM (Hora Chile).
3. **Cómputo (Cloud Run + Docker):** El código se empaqueta en un contenedor Docker y se ejecuta de forma efímera y escalable.
4. **Data Lake (Cloud Storage):** Almacena las capas de datos `raw`, `interim` y `processed` como respaldo histórico.
5. **Data Warehouse (BigQuery):** Carga incremental mediante operaciones `MERGE` (Upsert) garantizando datos únicos y limpios.
6. **Visualización (Power BI):** (En desarrollo) Conexión en modo Import para análisis diario.

## Tecnologías Utilizadas
* **Lenguaje:** Python 3.10
* **Contenedores:** Docker, Google Artifact Registry
* **GCP:** Cloud Run Jobs, Cloud Scheduler, Cloud Storage, BigQuery
* **Librerías clave:** `pandas`, `pandas-gbq`, `beautifulsoup4`, `gcsfs`
