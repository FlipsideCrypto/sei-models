-- depends_on: {{ ref('bronze_evm__streamline_receipts') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number)",
    tags = ['core']
) }}



WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key,
        DATA AS receipts_json,
        utils.udf_hex_to_int(DATA:transactionIndex::string) AS tx_position, -- this is different than FSC_EVM as on SEI the cosmos txs are included in the positioning. We need to join on tx index instead. This may change when the cosmos side is deprecated.
        DATA:transactionHash::string AS tx_hash,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze_evm__streamline_receipts') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND DATA IS NOT NULL
{% else %}
    {{ ref('bronze_evm__streamline_fr_receipts') }}
    WHERE DATA IS NOT NULL
{% endif %}
)
SELECT 
    block_number,
    tx_hash,
    partition_key,
    tx_position,
    receipts_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_position']) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
qualify((ROW_NUMBER() over (PARTITION BY block_number, tx_position ORDER BY _inserted_timestamp DESC)) = 1
    AND (ROW_NUMBER() over (PARTITION BY tx_hash ORDER BY block_number DESC)) = 1)