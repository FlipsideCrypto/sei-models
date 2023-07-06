{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'txcount', 'exploded_key', '[\"result\", \"total_count\"]', 'producer_limit_size', {{var('producer_limit_size','10000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','10000')}}, 'batch_call_limit', {{var('batch_call_limit','100')}}))",
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
            '"1000",',
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
