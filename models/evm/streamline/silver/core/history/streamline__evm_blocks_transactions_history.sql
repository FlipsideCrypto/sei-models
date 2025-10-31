{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_blocks_transactions",
        "sql_limit" :"1000000",
        "producer_batch_size" :"3000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(['result', 'result.transactions'])
    }
    ),
    tags = ['streamline_core_evm_history']
) }}

WITH to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__evm_blocks") }}
    WHERE block_number > 160000000
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_evm_blocks") }} b
    INNER JOIN {{ ref("streamline__complete_evm_transactions") }} t USING (block_number)
    WHERE block_number > 160000000
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
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            'Vault/prod/sei/quicknode/mainnet'
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
        limit 1000000