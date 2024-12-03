{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

SELECT
    contract_address,
    admin,
    init_by_account_address,
    label,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS contract_config_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze_api__get_contract_labels'
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
