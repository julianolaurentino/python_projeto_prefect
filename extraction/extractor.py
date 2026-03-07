import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from loguru import logger

from api_client import fetch_all_pages

load_dotenv()

# -- Constantes --

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
DUCKDB_PATH   = Path(os.getenv("DUCKDB_PATH",  "data/warehouse/aic.duckdb"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", 0)) or None  # 0 = todas as páginas


# -- Funções auxiliares e de persistência --

def save_raw_json(records: list[dict], extracted_at: str) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    filename = RAW_DATA_DIR / f"artworks_{extracted_at}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON bruto salvo em: {filename}")
    return filename


def load_to_bronze(records: list[dict], extracted_at: str) -> None:

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_artworks (
            id                   INTEGER,
            title                VARCHAR,
            artist_title         VARCHAR,
            artist_display       VARCHAR,
            date_start           INTEGER,
            date_end             INTEGER,
            date_display         VARCHAR,
            medium_display       VARCHAR,
            artwork_type_title   VARCHAR,
            department_title     VARCHAR,
            place_of_origin      VARCHAR,
            is_public_domain     BOOLEAN,
            is_on_view           BOOLEAN,
            colorfulness         DOUBLE,
            style_title          VARCHAR,
            classification_title VARCHAR,
            term_titles          VARCHAR,   
            dimensions           VARCHAR,
            credit_line          VARCHAR,
            _extracted_at        TIMESTAMP,
            _source              VARCHAR
        )
    """)

    # Monta os registros para inserção
    rows = []
    for r in records:
        rows.append((
            r.get("id"),
            r.get("title"),
            r.get("artist_title"),
            r.get("artist_display"),
            r.get("date_start"),
            r.get("date_end"),
            r.get("date_display"),
            r.get("medium_display"),
            r.get("artwork_type_title"),
            r.get("department_title"),
            r.get("place_of_origin"),
            r.get("is_public_domain"),
            r.get("is_on_view"),
            r.get("colorfulness"),
            r.get("style_title"),
            r.get("classification_title"),
            json.dumps(r.get("term_titles", []), ensure_ascii=False),
            r.get("dimensions"),
            r.get("credit_line"),
            extracted_at,
            "api.artic.edu",
        ))

    con.executemany("""
        INSERT INTO bronze.raw_artworks VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, rows)

    total = con.execute("SELECT COUNT(*) FROM bronze.raw_artworks").fetchone()[0]
    logger.info(f"Bronze carregado: {len(rows)} novos registros | Total na tabela: {total}")
    con.close()


def run() -> None:
    extracted_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info(f"Iniciando extração — {extracted_at}")

    records = fetch_all_pages(max_pages=MAX_PAGES)
    logger.info(f"Total extraído da API: {len(records)} registros")

    save_raw_json(records, extracted_at)
    load_to_bronze(records, extracted_at)

    logger.info("Extração concluída com sucesso.")


if __name__ == "__main__":
    run()