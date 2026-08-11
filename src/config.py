from pathlib import Path

BASE_URL = "https://api.artic.edu/api/v1/artworks"

FIELDS = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = str(PROJECT_ROOT / "data" / "warehouse" / "aic.duckdb")
BRONZE_PATH = str(PROJECT_ROOT / "data" / "bronze")
