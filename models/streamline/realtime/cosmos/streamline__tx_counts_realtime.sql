{{ config (
    materialized = "incremental",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}
-- dep
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}

{% if is_incremental() %}
EXCEPT
SELECT
    block_number
FROM
    {{ this }}
{% endif %}
ORDER BY
    1 DESC
LIMIT
    200
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    block_number AS block_number,
    live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                '1',
                '1',
                'asc'
            )
        ),
        'vault/prod/sei/quicknode/arctic1'
    ) :data :result :total_count :: INT AS tx_count
FROM
    blocks
ORDER BY
    block_number
