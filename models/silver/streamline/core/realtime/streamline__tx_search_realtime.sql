{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'tx_search', 'exploded_key', '[\"result\", \"txs\"]', 'producer_limit_size', {{var('producer_limit_size','1000')}}, 'producer_batch_size', {{var('producer_batch_size','100')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}


WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
blocks AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_tx_search") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
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
            '"1"',
            ',',
            '"1000"',
            ',',
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
