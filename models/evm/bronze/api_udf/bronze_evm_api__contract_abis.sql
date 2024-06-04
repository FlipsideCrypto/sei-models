{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        contract_address
    FROM
        {{ ref('silver_evm__relevant_contracts') }}
    WHERE
        total_interaction_count >= 100

{% if is_incremental() %}
EXCEPT
SELECT
    contract_address
FROM
    {{ this }}
{% endif %}
LIMIT
    50
), row_nos AS (
    SELECT
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no
    FROM
        base
),
batched AS ({% for item in range(51) %}
SELECT
    rn.contract_address, CONCAT('https://seitrace.com/pacific-1/api/v2/smart-contracts/', contract_address) AS url, IFNULL(live.udf_api(url) :data :abi, ARRAY_CONSTRUCT('ABI unavailable')) AS abi_data, SYSDATE() AS _inserted_timestamp
FROM
    row_nos rn
WHERE
    row_no = {{ item }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    contract_address,
    abi_data,
    _inserted_timestamp
FROM
    batched
