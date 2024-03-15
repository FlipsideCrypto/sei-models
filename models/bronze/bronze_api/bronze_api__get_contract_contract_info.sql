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
LEFT JOIN silver.contract_contract_info b
ON A.nft_address = b.contract_address
WHERE
    (
        b.contract_address IS NULL
        OR b._inserted_timestamp < CURRENT_DATE -7
    )
    AND COALESCE(
        b.code,
        999
    ) = 999
ORDER BY
    COALESCE(
        b._inserted_timestamp,
        '1900-01-01'
    )
{% endif %}
),
base AS (
    SELECT
        contract_address,
        {{ target.database }}.live.udf_api(
            'get',
            '{service}/{Authentication}' || '/cosmwasm/wasm/v1/contract/' || contract_address || '/smart/ewogICJjb250cmFjdF9pbmZvIjoge30KfQ==',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),{},
            'Vault/prod/sei/node/rest/mainnet'
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
