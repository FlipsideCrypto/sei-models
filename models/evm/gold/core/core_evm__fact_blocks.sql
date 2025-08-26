{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core']
) }}

SELECT
    b.block_number,
    block_json :hash :: STRING AS block_hash,
    utils.udf_hex_to_int(
        block_json :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    'mainnet' AS network,
    ARRAY_SIZE(
        block_json :transactions
    ) AS tx_count,
    utils.udf_hex_to_int(
        block_json :size :: STRING
    ) :: bigint AS SIZE,
    block_json :miner :: STRING AS miner,
    block_json :mixHash :: STRING AS mix_hash,
    block_json :extraData :: STRING AS extra_data,
    block_json :parentHash :: STRING AS parent_hash,
    utils.udf_hex_to_int(
        block_json :gasUsed :: STRING
    ) :: bigint AS gas_used,
    utils.udf_hex_to_int(
        block_json :gasLimit :: STRING
    ) :: bigint AS gas_limit,
    utils.udf_hex_to_int(
        block_json :baseFeePerGas :: STRING
    ) :: bigint AS base_fee_per_gas,
    TRY_TO_NUMBER(utils.udf_hex_to_int(
        block_json :difficulty :: STRING
    )) AS difficulty,
    TRY_TO_NUMBER(utils.udf_hex_to_int(
        block_json :totalDifficulty :: STRING
    )) AS total_difficulty,
    block_json :sha3Uncles :: STRING AS sha3_uncles,
    block_json :uncles AS uncle_blocks,
    utils.udf_hex_to_int(
        block_json :nonce :: STRING
    ) :: bigint AS nonce,
    block_json :receiptsRoot :: STRING AS receipts_root,
    block_json :stateRoot :: STRING AS state_root,
    block_json :transactionsRoot :: STRING AS transactions_root,
    block_json :logsBloom :: STRING AS logs_bloom,
    {{ dbt_utils.generate_surrogate_key(['b.block_number']) }} AS fact_blocks_id,
    {% if is_incremental() %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    {{ ref('silver_evm__blocks') }} b
WHERE 1=1

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
    )
{% endif %}
