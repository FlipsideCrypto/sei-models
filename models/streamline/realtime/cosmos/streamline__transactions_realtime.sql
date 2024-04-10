{{ config (
    materialized = "incremental",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

WITH blocks AS (

    SELECT
        A.block_number,
        tx_count
    FROM
        {{ ref("streamline__tx_counts_realtime") }} A

{% if is_incremental() %}
LEFT JOIN {{ this }}
b
ON A.block_number = b.block_number
WHERE
    b.block_number IS NULL
    AND tx_count > 0
    AND DATA :error IS NULL
{% else %}
WHERE
    tx_count > 0
{% endif %}
ORDER BY
    1 DESC
LIMIT
    100
), numbers AS (
    -- Recursive CTE to generate numbers. We'll use the maximum txcount value to limit our recursion.
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(tx_count) / 100.0)
            FROM
                blocks)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number AS block_number,
                n.n AS page_number
            FROM
                blocks tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.tx_count % 100 = 0 THEN tt.tx_count / 100
                    ELSE FLOOR(
                        tt.tx_count / 100
                    ) + 1
                END
            WHERE
                tt.tx_count > 0
        )
    SELECT
        ROUND(
            block_number,
            -3
        ) AS partition_key,
        block_number AS block_number,
        page_number AS page,
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
                    page_number :: STRING,
                    '100',
                    'asc'
                )
            ),
            'vault/prod/sei/quicknode/arctic1'
        ) AS DATA,
        SYSDATE() AS _inserted_timestamp
    FROM
        blocks_with_page_numbers
    ORDER BY
        block_number
