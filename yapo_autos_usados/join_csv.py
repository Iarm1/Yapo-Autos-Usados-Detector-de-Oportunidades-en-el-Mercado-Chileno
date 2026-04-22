import pandas as pd
from pathlib import Path

# Raíz del proyecto = dos niveles arriba del script
BUCKET = "yapo-autos-datalake"
listing = pd.read_csv(f"gs://{BUCKET}/raw/yapo_listing_raw.csv")
detail  = pd.read_csv(f"gs://{BUCKET}/raw/yapo_detail_raw.csv")

full = pd.merge(listing, detail, on="id", how="left")

print(f"Listing:  {listing.shape}")
print(f"Detail:   {detail.shape}")
print(f"Full:     {full.shape}")
print(f"Nulos por columna:\n{full.isnull().sum()}")

full.to_csv(f"gs://{BUCKET}/interim/yapo_full_raw.csv", index=False)
print("\n✅ Guardado en interim/yapo_full_raw.csv")
