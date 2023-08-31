{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'txcount', 'sql_limit', {{var('sql_limit','300000')}}, 'producer_batch_size', {{var('producer_batch_size','300000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'batch_call_limit', {{var('batch_call_limit','100')}}, 'exploded_key', '[\"result\", \"total_count\"]', 'call_type', 'batch'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_txcount") }}
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
            ',',
            '"1",',
            '"1",',
            '"asc"',
            '],"id":"',
            block_number :: STRING,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number ASC
