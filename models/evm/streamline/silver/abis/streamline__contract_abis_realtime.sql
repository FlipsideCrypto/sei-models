{{ config (
    materialized = "view",
    tags = ['noncore']
) }}

WITH recent_relevant_contracts AS (

    SELECT
        contract_address,
        total_interaction_count,
        GREATEST(
            max_inserted_timestamp_logs,
            max_inserted_timestamp_traces
        ) max_inserted_timestamp
    FROM
        {{ ref('silver_evm__relevant_contracts') }} C
        LEFT JOIN {{ ref("streamline__complete_contract_abis") }}
        s USING (contract_address)
    WHERE
        s.contract_address IS NULL
        AND total_interaction_count > 1000
    ORDER BY
        total_interaction_count DESC
    LIMIT
        2000
), all_contracts AS (
    SELECT
        contract_address
    FROM
        recent_relevant_contracts

{% if is_incremental() %}
UNION
SELECT
    contract_address
FROM
    {{ ref('_retry_abis') }}
{% endif %}
)
SELECT
    contract_address,
    DATE_PART('EPOCH_SECONDS', sysdate()::date) :: INT AS partition_key,
    live.udf_api(
        'GET',
        CONCAT(
            'https://api.etherscan.io/v2/api?chainid=1329&module=contract&action=getabi&address=',
            contract_address,
            '&apikey={KEY}'
        ),
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        {},
        'Vault/prod/sei/etherscan'
    ) AS request
FROM
    all_contracts

{# Streamline Function Call #}
{% if execute %}
    {% set params = { 
        "external_table" :"contract_abis",
        "sql_limit" : 2500,
        "producer_batch_size" : 100,
        "worker_batch_size" : 100,
        "async_concurrent_requests" : 5,
        "sql_source" : 'contract_abis_realtime'
    } %}

    {% set function_call_sql %}
    {{ fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = this.schema ~ "." ~ this.identifier,
        params = params
    ) }}
    {% endset %}
    
    {% do run_query(function_call_sql) %}
    {{ log("Streamline function call: " ~ function_call_sql, info=true) }}
{% endif %}