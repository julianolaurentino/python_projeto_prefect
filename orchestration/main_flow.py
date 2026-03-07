"""
main_flow.py
Flow principal do pipeline ELT do Art Institute of Chicago.

Sequência:
    1. extract   — busca obras na API do AIC
    2. load      — salva JSON bruto + carrega no Bronze (DuckDB)
    3. transform — executa dbt run (Bronze → Silver → Gold)
    4. test      — executa dbt test para validar os modelos
"""

import os

from dotenv import load_dotenv
from prefect import flow
from prefect.logging import get_run_logger

from tasks.extract_task import extract_artworks
from tasks.load_task import load_artworks
from tasks.transform_task import run_dbt, run_dbt_test


@flow(
    name="aic_elt_pipeline",
    description="Pipeline ELT do Art Institute of Chicago com arquitetura medalhão.",
    log_prints=True,
)
def aic_elt_pipeline(max_pages: int = None) -> None:
    """
    Executa o pipeline ELT completo.

    Args:
        max_pages: Número máximo de páginas da API a buscar.
                   None = todas as páginas (~131k obras).
                   Use 2 ou 3 para testes rápidos.
    """
    # Logger nativo do Prefect — aparece na UI em Flow Runs
    logger = get_run_logger()

    logger.info("=" * 50)
    logger.info("Iniciando pipeline AIC ELT")
    logger.info(f"max_pages={max_pages}")
    logger.info("=" * 50)

    # 1. Extract
    records = extract_artworks(max_pages=max_pages)

    # 2. Load → Bronze
    extracted_at = load_artworks(records)

    # 3. Transform → Silver + Gold (dbt run)
    run_dbt(extracted_at)

    # 4. Validação (dbt test)
    run_dbt_test()

    logger.info("Pipeline concluído com sucesso!")


if __name__ == "__main__":
    # Execução local para testes
    # load_dotenv() aqui garante que o .env é carregado antes de qualquer coisa
    load_dotenv()
    max_pages = int(os.getenv("MAX_PAGES", 0)) or None
    aic_elt_pipeline(max_pages=max_pages)