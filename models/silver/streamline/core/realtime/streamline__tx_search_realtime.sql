{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'sql_limit', {{var('sql_limit','6000000')}}, 'producer_batch_size', {{var('producer_batch_size','3000000')}}, 'worker_batch_size', {{var('worker_batch_size','15000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}, 'exploded_key', '[\"result\", \"txs\"]', 'call_type', 'batch'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_txcount") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_tx_search") }}
),
transactions_counts_by_block AS (
    SELECT
        tc.block_number,
        tc.data :: INTEGER AS txcount
    FROM
        {{ ref("bronze__streamline_txcount") }}
        tc
        INNER JOIN blocks b
        ON tc.block_number = b.block_number
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
                CEIL(MAX(txcount) / 100.0)
            FROM
                transactions_counts_by_block)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number AS block_number,
                n.n AS page_number
            FROM
                transactions_counts_by_block tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.txcount % 100 = 0 THEN tt.txcount / 100
                    ELSE FLOOR(
                        tt.txcount / 100
                    ) + 1
                END
        )
    SELECT
        PARSE_JSON(
            CONCAT(
                '{"jsonrpc": "2.0",',
                '"method": "tx_search", "params":["',
                'tx.height=',
                block_number :: STRING,
                '",',
                TRUE,
                ',"',
                page_number :: STRING,
                '",',
                '"100",',
                '"asc"',
                '],"id":"',
                block_number :: STRING,
                '"}'
            )
        ) AS request
    FROM
        blocks_with_page_numbers
    ORDER BY
        block_number ASC
