{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'evm_blocks_testnet', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','Vault/prod/sei/quicknode/arctic1'))",
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
        {{ ref("streamline_evm__complete_blocks") }}
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
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)) :: STRING
        ) AS request
        FROM
            to_do
        ORDER BY
            partition_key ASC
