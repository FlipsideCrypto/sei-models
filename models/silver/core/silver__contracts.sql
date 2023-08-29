{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    incremental_strategy = 'merge'
) }}

SELECT
    contract_address,
    admin,
    init_by_account_address,
    init_by_block_timestamp,
    label,
    _inserted_timestamp
FROM
    {{ ref(
        'bronze_api__get_contract_list'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY contract_address
ORDER BY
    _inserted_timestamp DESC) = 1)
