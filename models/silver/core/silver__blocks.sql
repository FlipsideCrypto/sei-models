{{ config(
    materialized = 'view',
    tags = ['v2']
) }}

WITH base AS (

    SELECT
        DATA,
        block_number AS block_id,
        DATA :data: "result" :block :header AS header,
        COALESCE(ARRAY_SIZE(DATA :data :result :block :data :txs) :: NUMBER, 0) AS tx_count,
        header :chain_id :: STRING AS chain_id,
        header :time :: datetime AS block_timestamp,
        header :proposer_address :: STRING AS proposer_address,
        header :validators_hash :: STRING AS validator_hash,
        _inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('streamline__blocks_realtime') }}
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
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
