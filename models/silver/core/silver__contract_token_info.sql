{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    incremental_strategy = 'merge',
    tags = ['noncore']
) }}

SELECT
    contract_address,
    DATA :data :data AS DATA,
    DATA :data :code AS code,
    DATA :data :details AS details,
    DATA :data :message AS message,
    _inserted_timestamp
FROM
    {{ ref(
        'bronze_api__get_contract_token_info'
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
