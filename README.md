# AIC ELT Pipeline

Pipeline de dados com arquitetura medalhão utilizando a API pública do **Art Institute of Chicago**, construído como projeto de portfólio em Engenharia de Dados.

---

## Arquitetura

![img](https://i.postimg.cc/Kc0LvXV0/Group-4.png)


### Camadas Medalhão

| Camada | Materialização | Descrição |
|--------|---------------|-----------|
| 🥉 **Bronze** | `view` | Dados brutos da API, sem alterações. Preserva histórico. |
| 🥈 **Silver** | `table` | Limpeza, tipagem, deduplicação e campo `historical_period`. |
| 🥇 **Gold** | `table` | Agregações analíticas prontas para consumo. |

---

## Stack

| Ferramenta | Função |
|-----------|--------|
| **Python 3.11** | Extração e carga |
| **DuckDB** | Banco de dados local (warehouse) |
| **dbt-duckdb** | Transformações SQL (Silver + Gold) |
| **Prefect 3** | Orquestração do pipeline |
| **Docker** | Containerização |

---

## Estrutura de Pastas

```
aic-elt-pipeline/
├── src/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── api_client.py
│   ├── extractor.py
│   └── ...
│
├── data/
│   ├── raw/
│   └── warehouse/
│       ├── aic.duckdb
│       └── bronze/
│
├── dbt/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/
│       │   └── bronze_artworks.sql
│       ├── silver/
│       │   └── silver_artworks.sql
│       └── gold/
│           ├── gold_artworks_by_type.sql
│           ├── gold_artworks_by_period.sql
│           ├── gold_artworks_by_department.sql
│           └── gold_top_artists.sql
│
├── pipelines/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── __init__.py
│   ├── flows/
│   │   └── main_flow.py
│   └── tasks/
│       ├── extract_task.py
│       ├── load_task.py
│       └── transform_task.py
│
├── docker-compose.yml
├── requirements.txt       # agregador opcional (inclui src/pipelines/dbt)
├── .gitignore
└── README.md
```

---

## Como Executar

### Pré-requisitos

- Python 3.12+
- ambiente virtual (`venv`)
- permissões de escrita na pasta do projeto (especialmente em `data/`, `data/warehouse/`, `data/bronze/` e `data/dbt/`)

### 1. Entre na raiz do projeto

```bash
cd /home/obzen/Documentos/workspace/python_pipeline_aic
```

### 2. Crie e ative o ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências

```bash
pip install -U pip
pip install -r pipelines/requirements.txt
```

### 4. Configure o arquivo `.env`

```bash
cat > .env <<EOF
MAX_PAGES=1
EOF
```

- `MAX_PAGES=1` é recomendado para testes rápidos (~100 obras).
- `MAX_PAGES=0` executa a extração completa (muitas páginas e mais tempo).

### 5. Execute o fluxo completo

```bash
python -m pipelines.flows.main_flow
```

Esse comando executa o processo atual do projeto:
1. Extração da API do AIC
2. Persistência do JSON bruto em `data/bronze/`
3. Carga no DuckDB em `data/warehouse/aic.duckdb`
4. `dbt run` para Bronze → Silver → Gold
5. `dbt test` para validar os modelos

### 6. Saídas esperadas

- JSON bruto: `data/bronze/artworks_*.json`
- Banco DuckDB: `data/warehouse/aic.duckdb`
- Artefatos e logs do dbt: `data/dbt/target/` e `data/dbt/logs/`

### 7. Execução do dbt separadamente (opcional)

```bash
dbt run --project-dir dbt --profiles-dir dbt --target-path data/dbt/target --log-path data/dbt/logs
dbt test --project-dir dbt --profiles-dir dbt --target-path data/dbt/target --log-path data/dbt/logs
```

---

## Modelos Gold

| Modelo | Descrição |
|--------|-----------|
| `gold_artworks_by_type` | Distribuição por tipo de obra com % do total |
| `gold_artworks_by_period` | Obras por período histórico (Medieval → Século XXI) |
| `gold_artworks_by_department` | Obras por departamento com obras em exibição |
| `gold_top_artists` | Top 100 artistas com mais obras no acervo |

---

## Fonte de Dados

[Art Institute of Chicago API](https://api.artic.edu/docs/) — API pública com mais de 131.000 obras de arte, sem necessidade de autenticação.

---

## Autor

[Juliano Laurentino](https://www.linkedin.com/in/julianolaurentinodasilva/)
