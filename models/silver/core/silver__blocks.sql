{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}
WITH base AS (

    SELECT
        DATA :result :block :header :height :: INT AS block_id,
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
    _inserted_timestamp >= DATEADD(
        MINUTE,
        -90,(
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
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
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
WHERE
    block_id IS NOT NULL
