{{ config(
    materialized = 'table',
    tags = ['daily','recent_test']
) }}

WITH base AS (

    SELECT
        contract_address,
        DATA :decimals :: INT AS decimals,
        DATA :name :: STRING AS token_name,
        DATA :symbol :: STRING AS symbol,
        'contract' source
    FROM
        {{ ref('silver__contract_token_info') }}
    WHERE
        DATA IS NOT NULL
    UNION ALL
    SELECT
        address,
        DECIMAL,
        label AS token_name,
        project_name AS symbol,
        'osmosis' source
    FROM
        {{ source(
            'osmosis',
            'asset_metadata'
        ) }}
    WHERE
        address LIKE 'ibc%'
    UNION ALL
    SELECT
        contract_address,
        decimals,
        NAME,
        symbol,
        'price' source
    FROM
        {{ ref('silver__prices_api') }}
)
SELECT
    contract_address AS currency,
    decimals,
    token_name,
    symbol,
    {{ dbt_utils.generate_surrogate_key(
        ['currency']
    ) }} AS asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY contract_address
ORDER BY
    CASE
    WHEN source = 'contract' THEN 'a'
    WHEN source = 'osmosis' THEN 'b'
    WHEN source = 'price' THEN 'c'END) = 1)
