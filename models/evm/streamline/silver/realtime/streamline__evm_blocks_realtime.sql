{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_blocks",
        "sql_limit" :"25000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"10000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_core_evm_realtime']
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
        {{ ref("streamline__evm_blocks") }}
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
        {{ ref("streamline__complete_evm_blocks") }}
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
    block_number,
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
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
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)),
            'Vault/prod/sei/quicknode/mainnet'
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
