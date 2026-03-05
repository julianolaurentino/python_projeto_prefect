"""
api_client.py
Responsável por toda comunicação com a API do Art Institute of Chicago.
"""

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger

BASE_URL = "https://api.artic.edu/api/v1/artworks"

# Campos que vamos extrair da API
FIELDS = ",".join([
    "id",
    "title",
    "artist_title",
    "artist_display",
    "date_start",
    "date_end",
    "date_display",
    "medium_display",
    "artwork_type_title",
    "department_title",
    "place_of_origin",
    "is_public_domain",
    "is_on_view",
    "colorfulness",
    "style_title",
    "classification_title",
    "term_titles",
    "dimensions",
    "credit_line",
])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_page(page: int, limit: int = 100) -> dict:
    """
    Busca uma página de obras de arte da API.

    Args:
        page:  Número da página (começa em 1).
        limit: Quantidade de registros por página (máx. 100).

    Returns:
        JSON completo da resposta com 'data' e 'pagination'.
    """
    params = {
        "page": page,
        "limit": limit,
        "fields": FIELDS,
    }

    logger.info(f"Buscando página {page} (limit={limit})")
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def fetch_all_pages(max_pages: int = None, limit: int = 100) -> list[dict]:
    """
    Itera sobre todas as páginas da API e retorna todos os registros.

    Args:
        max_pages: Limite de páginas a buscar (None = todas).
        limit:     Registros por página.

    Returns:
        Lista de dicts com todos os artworks extraídos.
    """
    all_records = []
    page = 1

    while True:
        data = fetch_page(page=page, limit=limit)
        records = data.get("data", [])
        pagination = data.get("pagination", {})

        all_records.extend(records)
        logger.info(
            f"Página {page}/{pagination.get('total_pages')} — "
            f"{len(all_records)} registros acumulados"
        )

        # Verifica se existe próxima página
        if not pagination.get("next_url"):
            logger.info("Última página atingida.")
            break

        if max_pages and page >= max_pages:
            logger.info(f"Limite de {max_pages} páginas atingido.")
            break

        page += 1

    return all_records