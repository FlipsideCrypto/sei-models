{{ config (
    materialized = "incremental",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

WITH to_do AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks_realtime") }}
    WHERE
        block_number IS NOT NULL

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
    100
), ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
)
SELECT
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    block_number AS block_number,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))),
            'vault/prod/sei/quicknode/arctic1'
        ) AS DATA
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
