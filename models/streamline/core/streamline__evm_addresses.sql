{{ config (
    materialized = "incremental",
    unique_key = "evm_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["block_timestamp_associated","block_id_associated","inserted_timestamp"],
) }}

SELECT
    from_address AS evm_address,
    block_timestamp AS block_timestamp_associated,
    block_number AS block_id_associated,
    {{ dbt_utils.generate_surrogate_key(
        ['evm_address']
    ) }} AS streamline__evm_addresses_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver_evm__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY evm_address
ORDER BY
    block_timestamp)) = 1
