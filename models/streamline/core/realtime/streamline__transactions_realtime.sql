{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"100000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"100",
        "exploded_key": "[\"result\", \"txs\"]",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_transactions') }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        A.block_number,
        tx_count
    FROM
        {{ ref("streamline__complete_tx_counts") }} A
    WHERE
        tx_count > 0
),
numbers AS (
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
            EXCEPT
            SELECT
                block_number,
                page_number
            FROM
                {{ ref("streamline__complete_transactions") }}
            ORDER BY
                1 DESC
            LIMIT
                10
        )
    SELECT
        ROUND(
            block_number,
            -3
        ) :: INT AS partition_key,
        live.udf_api(
            'POST',
            '{service}/{Authentication}',
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
            'Vault/prod/sei/allthatnode/mainnet'
        ) AS request,
        page_number,
        block_number AS block_number_requested
    FROM
        blocks_with_page_numbers
    ORDER BY
        block_number
