{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

SELECT
    contract_address,
    DATA :data :data AS DATA,
    DATA :data :code AS code,
    DATA :data :details AS details,
    DATA :data :message AS message,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS contract_info_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze_api__get_contract_info'
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
