{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_v2",
        "sql_limit" :"10000",
        "producer_batch_size" :"2000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_blocks') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
    ORDER BY
        1
)
SELECT
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
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'block',
            'params',
            ARRAY_CONSTRUCT(
                block_number :: STRING
            )
        ),
        'Vault/prod/sei/quicknode/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
