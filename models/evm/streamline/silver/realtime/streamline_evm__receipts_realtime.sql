{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'evm_receipts_testnet', 'exploded_key','[\"result\"]', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','Vault/prod/sei/quicknode/arctic1'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_evm_block_lookback") }}
),
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline_evm__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline_evm__complete_receipts") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do {# add retry here #}
)
SELECT
    block_number AS partition_key,
    OBJECT_CONSTRUCT(
        'method',
        'POST',
        'url',
        '{Service}/{Authentication}',
        'headers',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        'params',
        PARSE_JSON('{}'),
        'data',
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))) :: STRING
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
