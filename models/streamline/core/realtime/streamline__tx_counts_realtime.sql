{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"txcount_v2",
        "sql_limit" :"10000",
        "producer_batch_size" :"2000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number :: INT
    FROM
        {{ ref("streamline__complete_tx_counts") }}
    ORDER BY
        1
),
retry AS (
    SELECT
        A.block_number
    FROM
        {{ ref("streamline__complete_tx_counts") }} A
        JOIN {{ ref("silver__blockchain") }}
        b
        ON A.block_number = b.block_id
    WHERE
        A.tx_count <> b.num_txs
),
combo AS (
    SELECT
        block_number
    FROM
        blocks
    UNION
    SELECT
        block_number
    FROM
        retry
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    live.udf_api(
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
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                '1',
                '1',
                'asc'
            )
        ),
        'Vault/prod/sei/quicknode/mainnet'
    ) AS request,
    block_number
FROM
    combo
ORDER BY
    block_number
