{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        header,
        tx_count,
        chain_id,
        block_timestamp,
        header :proposer_address :: STRING AS proposer_address,
        header :validators_hash :: STRING AS validator_hash,
        _inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}

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

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_id,
    block_timestamp,
    'sei' AS blockchain,
    chain_id,
    tx_count,
    proposer_address,
    validator_hash,
    header,
    _inserted_timestamp
FROM
    base
