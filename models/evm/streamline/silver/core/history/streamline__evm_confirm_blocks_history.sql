{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_confirm_blocks",
        "sql_limit" :"1000000",
        "producer_batch_size" :"3000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_core_evm_history']
) }}

WITH look_back AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_evm_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
),
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__evm_blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number < (
            SELECT
                block_number
            FROM
                look_back
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_evm_confirmed_blocks") }}
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
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
            'application/json',
            'fsc-quantum-state', 'streamline'
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
            block_number asc
    limit 1000000