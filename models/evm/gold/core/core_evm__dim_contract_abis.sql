{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(contract_address,bytecode)",
    tags = ['abis']
) }}

SELECT
    contract_address,
    DATA AS abi,
    abi_source,
    bytecode,
    abis_id AS dim_contract_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver_evm__abis') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp),'1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }}
    )
{% endif %}