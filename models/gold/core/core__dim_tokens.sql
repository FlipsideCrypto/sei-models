{{ config(
    materialized = 'view',
    tags = ['core','recent_test']
) }}

SELECT
    currency,
    decimals,
    token_name,
    symbol,
    COALESCE (
        asset_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['currency']
        ) }}
    ) AS dim_tokens_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__asset_metadata') }}
