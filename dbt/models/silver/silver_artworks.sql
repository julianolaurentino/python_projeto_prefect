-- silver/silver_artworks.sql
-- Limpeza, tipagem e padronização dos dados brutos
-- Remove duplicatas, trata nulos e normaliza campos de texto

WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ ref('bronze_artworks') }}
    WHERE id IS NOT NULL
),

typed AS (
    SELECT
        *,
        TRY_CAST(NULLIF(TRIM(date_start), '') AS INTEGER) AS date_start_int,
        TRY_CAST(NULLIF(TRIM(date_end), '') AS INTEGER) AS date_end_int,
        TRY_CAST(NULLIF(TRIM(colorfulness), '') AS DOUBLE) AS colorfulness_num,
        COALESCE(TRY_CAST(NULLIF(TRIM(is_public_domain), '') AS BOOLEAN), FALSE) AS is_public_domain_bool,
        COALESCE(TRY_CAST(NULLIF(TRIM(is_on_view), '') AS BOOLEAN), FALSE) AS is_on_view_bool
    FROM deduped
    WHERE rn = 1
),

cleaned AS (
    SELECT
        id,

        -- Texto: trim e nulos explícitos
        NULLIF(TRIM(title), '')                AS title,
        NULLIF(TRIM(artist_title), '')         AS artist_title,
        NULLIF(TRIM(artist_display), '')       AS artist_display,

        -- Datas: mantém inteiros, valida range mínimo
        CASE
            WHEN date_start_int BETWEEN -5000 AND 2100 THEN date_start_int
            ELSE NULL
        END                                     AS date_start,
        CASE
            WHEN date_end_int BETWEEN -5000 AND 2100 THEN date_end_int
            ELSE NULL
        END                                     AS date_end,
        NULLIF(TRIM(date_display), '')          AS date_display,

        -- Classificações
        NULLIF(TRIM(medium_display), '')        AS medium_display,
        NULLIF(TRIM(artwork_type_title), '')    AS artwork_type_title,
        NULLIF(TRIM(department_title), '')      AS department_title,
        NULLIF(TRIM(place_of_origin), '')       AS place_of_origin,
        NULLIF(TRIM(style_title), '')           AS style_title,
        NULLIF(TRIM(classification_title), '')  AS classification_title,
        NULLIF(TRIM(dimensions), '')            AS dimensions,
        NULLIF(TRIM(credit_line), '')           AS credit_line,

        -- Booleans com default false
        is_public_domain_bool                    AS is_public_domain,
        is_on_view_bool                          AS is_on_view,

        -- Colorfulness: nulo para valores negativos ou absurdos
        CASE
            WHEN colorfulness_num >= 0 THEN ROUND(colorfulness_num, 4)
            ELSE NULL
        END                                     AS colorfulness,

        -- term_titles: mantém como string JSON para Silver
        term_titles,

        -- Período histórico calculado a partir do date_start
        -- NULL explícito primeiro para evitar que NULLs caiam no ELSE
        CASE
            WHEN date_start_int IS NULL  THEN 'Período Desconhecido'
            WHEN date_start_int < 0      THEN 'Antes de Cristo'
            WHEN date_start_int < 1400   THEN 'Medieval e Anterior'
            WHEN date_start_int < 1600   THEN 'Renascimento'
            WHEN date_start_int < 1800   THEN 'Barroco e Iluminismo'
            WHEN date_start_int < 1900   THEN 'Século XIX'
            WHEN date_start_int < 1945   THEN 'Início Século XX'
            WHEN date_start_int < 2000   THEN 'Pós-Guerra e Contemporâneo'
            WHEN date_start_int >= 2000  THEN 'Século XXI'
            ELSE 'Período Desconhecido'
        END                                     AS historical_period,

        _extracted_at,
        _source

    FROM typed
)

SELECT * FROM cleaned