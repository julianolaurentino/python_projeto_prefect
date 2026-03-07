"""
tasks/transform_task.py
Task Prefect responsável por executar o dbt (Bronze → Silver → Gold).
"""

import os
import subprocess
from pathlib import Path

from prefect import task
from loguru import logger

DBT_PROJECT_DIR = Path(
    os.getenv("DBT_PROJECT_DIR", "/app/transform")  # docker default
    if os.path.exists("/app/transform")
    else str(Path(__file__).resolve().parents[3] / "transform")  # local fallback
)


@task(
    name="run_dbt",
    description="Executa dbt run para transformar Bronze → Silver → Gold.",
    retries=1,
    retry_delay_seconds=10,
    tags=["transform", "dbt", "silver", "gold"],
)
def run_dbt(extracted_at: str) -> None:
    """
    Executa o pipeline dbt completo.

    Args:
        extracted_at: Timestamp do run atual (usado apenas para logging).
    """
    logger.info(f"Iniciando dbt run | extracted_at={extracted_at}")

    env = os.environ.copy()

    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
        env=env,
    )

    # Loga a saída do dbt
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info(f"[dbt] {line}")

    if result.returncode != 0:
        logger.error(f"[dbt] ERRO:\n{result.stderr}")
        raise RuntimeError(f"dbt run falhou com código {result.returncode}")

    logger.info("dbt run concluído com sucesso")


@task(
    name="run_dbt_test",
    description="Executa dbt test para validar os modelos Silver e Gold.",
    retries=1,
    retry_delay_seconds=5,
    tags=["test", "dbt"],
)
def run_dbt_test() -> None:
    """Roda os testes definidos no sources.yml (unique, not_null, etc.)."""
    logger.info("Iniciando dbt test")

    result = subprocess.run(
        ["dbt", "test", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info(f"[dbt test] {line}")

    if result.returncode != 0:
        logger.warning(f"[dbt test] Alguns testes falharam:\n{result.stderr}")
        # Não interrompe o pipeline — apenas avisa
    else:
        logger.info("Todos os testes passaram")