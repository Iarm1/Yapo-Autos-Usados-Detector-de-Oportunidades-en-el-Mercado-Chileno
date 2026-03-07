import pandas as pd
from pathlib import Path

# Raíz del proyecto = dos niveles arriba del script
ROOT = Path(__file__).resolve().parent.parent

listing = pd.read_csv(ROOT / "data/raw/yapo_listing_raw.csv")
detail  = pd.read_csv(ROOT / "data/raw/yapo_detail_raw.csv")

full = pd.merge(listing, detail, on="id", how="left")

print(f"Listing:  {listing.shape}")
print(f"Detail:   {detail.shape}")
print(f"Full:     {full.shape}")
print(f"Nulos por columna:\n{full.isnull().sum()}")

full.to_csv(ROOT / "data/raw/yapo_full_raw.csv", index=False)
print("\n✅ Guardado en data/raw/yapo_full_raw.csv")
