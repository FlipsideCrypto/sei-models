{{ config (
    materialized = "incremental",
    unique_key = "created_contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_timestamp, tx_hash, created_contract_address, creator_address), SUBSTRING(created_contract_address, creator_address)",
    tags = ['recent_evm_test','core']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    to_address AS created_contract_address,
    from_address AS creator_address,
    input AS created_contract_input,
    modified_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['to_address']
    ) }} AS created_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core_evm__fact_traces') }}
WHERE
    TYPE ILIKE 'create%'
    AND to_address IS NOT NULL
    AND input IS NOT NULL
    AND input != '0x'
    AND tx_succeeded
    AND trace_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY created_contract_address
ORDER BY
    _inserted_timestamp DESC)) = 1
