{# Scores Variables #}
{% set SCORES_FULL_RELOAD_ENABLED = var('SCORES_FULL_RELOAD_ENABLED', false) %}
{% set SCORES_LIMIT_DAYS = var('SCORES_LIMIT_DAYS', 30) %}
{% set include_gaming_metrics = var('INCLUDE_GAMING_METRICS', false) %}
{% set GLOBAL_PROJECT_NAME = 'sei'%}

{{ config (
    materialized = "view",
    tags = ['silver','scores','phase_4']
) }}

{% if execute %}
    {{ log("==========================================", info=True) }}
    {{ log("Generating date spine for blockchain: " ~ GLOBAL_PROJECT_NAME, info=True) }}
    {{ log("Backfill mode: " ~ SCORES_FULL_RELOAD_ENABLED, info=True) }}
    {{ log("==========================================", info=True) }}
{% endif %}

WITH chain_dates AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        count(distinct date_trunc('hour', block_timestamp)) AS n_hours
    FROM {{ ref('core_evm__fact_blocks') }}

{% if not SCORES_FULL_RELOAD_ENABLED %}
    WHERE block_timestamp :: DATE > DATEADD('day', -120, SYSDATE() :: DATE)
{% endif %}
    GROUP BY ALL
),
date_spine AS (

{% if SCORES_FULL_RELOAD_ENABLED %}
    SELECT
        date_day
    FROM
        {{ ref('scores__dates') }}
    WHERE
        day_of_week = 1
        AND date_day < DATEADD('day', -90, SYSDATE() :: DATE) -- every sunday, excluding last 90 days
    UNION
{% endif %}

    SELECT
        date_day
    FROM
        {{ ref('scores__dates') }}
    WHERE
        date_day >= DATEADD('day', -90, SYSDATE() :: DATE)
        and date_day <= (SELECT MAX(block_date) FROM chain_dates where n_hours = 24)
),
day_of_chain AS (
    SELECT
        block_date,
        ROW_NUMBER() over (ORDER BY block_date ASC) AS chain_day
    FROM
        chain_dates
),
exclude_first_90_days AS (
    SELECT
        block_date
    FROM
        day_of_chain

{% if SCORES_FULL_RELOAD_ENABLED %}
    WHERE chain_day >= 90
{% endif %}

),
eligible_dates AS (
    SELECT
        block_date
    FROM
        exclude_first_90_days
    JOIN date_spine ON date_day = block_date
)
SELECT
    block_date
FROM
    eligible_dates
ORDER BY block_date ASC