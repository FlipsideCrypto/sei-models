{{ config(
    materialized = 'view'
) }}

SELECT
    currency,
    decimals,
    token_name,
    symbol
FROM
    {{ ref('silver__asset_metadata') }}
