{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}
WITH base AS (

    SELECT
        block_number AS block_id,
        DATA :result :block :header AS header,
        COALESCE(ARRAY_SIZE(DATA :result :block :data :txs) :: NUMBER, 0) AS tx_count,
        header :chain_id :: STRING AS chain_id,
        header :time :: datetime AS block_timestamp,
        header :proposer_address :: STRING AS proposer_address,
        header :validators_hash :: STRING AS validator_hash,
        _inserted_timestamp AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
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
