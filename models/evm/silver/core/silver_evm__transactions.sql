-- depends_on: {{ ref('bronze_evm__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['core']
) }}

WITH bronze_transactions AS (
    SELECT 
        block_number,
        partition_key,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                VALUE :data :transactionIndex :: STRING
            )
        ) AS tx_position, -- this is different than FSC_EVM as on SEI the cosmos txs are included in the positioning. We need to join on tx index instead. This may change when the cosmos side is deprecated.
        DATA AS transaction_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze_evm__streamline_transactions') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA IS NOT NULL
    {% else %}
    {{ ref('bronze_evm__streamline_fr_transactions') }}
    WHERE DATA IS NOT NULL
    {% endif %}
)
SELECT 
    block_number,
    partition_key,
    tx_position,
    transaction_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_position']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_transactions
WHERE tx_position IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY transactions_id ORDER BY _inserted_timestamp DESC) = 1