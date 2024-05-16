{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH contracts AS (

    SELECT
        top 20 A.contract_address
    FROM
        {{ ref('silver__contracts') }} A

{% if is_incremental() %}
LEFT JOIN silver.contract_config b
ON A.contract_address = b.contract_address
WHERE
    (
        b.contract_address IS NULL
        OR b._inserted_timestamp < CURRENT_DATE -7
    )
{% endif %}
),
base AS (
    SELECT
        contract_address,
        {{ target.database }}.live.udf_api(
            'get',
            '{Service}' || '/cosmwasm/wasm/v1/contract/' || contract_address || '/smart/ewogICJjb25maWciOiB7fQp9',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),{},
            'Vault/prod/sei/publicnode/mainnet/rest'
        ) AS DATA,
        SYSDATE() AS _inserted_timestamp
    FROM
        contracts
)
SELECT
    contract_address,
    DATA,
    _inserted_timestamp
FROM
    base
