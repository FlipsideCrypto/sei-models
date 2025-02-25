{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"sei_addresses",
        "sql_limit" :"350",
        "producer_batch_size" :"50",
        "worker_batch_size" :"50",
        "sql_source" :"{{this.identifier}}",
        "async_concurrent_requests": "5" }
    )
) }}
-- depends_on: {{ ref('streamline__evm_addresses') }}
-- depends_on: {{ ref('streamline__complete_sei_addresses') }}
WITH adds AS (

    SELECT
        evm_address
    FROM
        {{ ref("streamline__evm_addresses") }}
    INTERSECT
    SELECT
        evm_address
    FROM
        (
            SELECT
                origin_from_address AS evm_address
            FROM
                {{ ref('silver_evm_dex__swaps_combined') }}
        )
),
excepts AS (
    SELECT
        evm_address
    FROM
        adds
    EXCEPT
    SELECT
        evm_address
    FROM
        {{ ref("silver__address_mapping_onchain") }}
    EXCEPT
    SELECT
        evm_address
    FROM
        {{ ref("streamline__complete_sei_addresses") }}
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
    excepts
