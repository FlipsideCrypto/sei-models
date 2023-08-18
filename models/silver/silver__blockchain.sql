{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_api',
            'blockchain'
        ) }}

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
),
fin AS (
    SELECT
        VALUE :header :chain_id :: STRING AS chain_id,
        VALUE :header :height :: INT AS height,
        VALUE :header :time :: datetime AS block_timestamp,
        VALUE :block_size :: INT AS block_size,
        VALUE :header AS block_header,
        VALUE :block_id AS block_id,
        VALUE :num_txs :: INT AS num_txs,
        _inserted_timestamp
    FROM
        (
            SELECT
                DATA,
                _inserted_timestamp
            FROM
                base
        ),
        LATERAL FLATTEN(
            DATA,
            recursive => TRUE
        ) b
    WHERE
        b.path LIKE 'data%.result.block_metas%'
        AND INDEX IS NOT NULL
)
SELECT
    chain_id,
    height AS block_id,
    block_timestamp,
    block_size,
    block_header,
    block_id AS block_id_object,
    num_txs,
    _inserted_timestamp
FROM
    fin qualify(ROW_NUMBER() over(PARTITION BY height
ORDER BY
    _inserted_timestamp DESC) = 1)
