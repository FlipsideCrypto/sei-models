{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"sei_addresses",
        "sql_limit" :"500",
        "producer_batch_size" :"100",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__evm_addresses') }}
WITH adds AS (

    SELECT
        evm_address
    FROM
        {{ ref("streamline__evm_addresses") }}
    EXCEPT
    SELECT
        evm_address
    FROM
        {{ ref("silver__address_mapping_onchain") }}
)
SELECT
    REPLACE(SYSDATE() :: DATE :: STRING, '-') AS partition_key,
    evm_address,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'sei_getSeiAddress',
            'params',
            ARRAY_CONSTRUCT(
                evm_address
            )
        ),
        'Vault/prod/sei/quicknode/mainnet'
    ) AS request
FROM
    adds
