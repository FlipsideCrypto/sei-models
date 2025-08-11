{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['noncore']
) }}

WITH new_tokens AS (

    SELECT
        DISTINCT contract_address
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= CURRENT_DATE() - INTERVAL '7 days'
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
),
ready_reads AS (
    SELECT
        contract_address,
        '0xfc0c546a' AS function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, 'latest']
        ) AS rpc_request
    FROM
        new_tokens
),
node_call AS (
    SELECT
        contract_address,
        live.udf_api(
            'POST',
            '{Service}/{Authentication}',{},
            rpc_request,
            'Vault/prod/sei/quicknode/mainnet'
        ) AS response,
        '0x' || LOWER(SUBSTR(response :data :result :: STRING, 27, 40)) AS token_address
    FROM
        ready_reads
)
SELECT
    response,
    contract_address,
    IFF(
        token_address = '0x0000000000000000000000000000000000000000',
        '0xe30fedd158a2e3b13e9badaeabafc5516e95e8c7',
        token_address
    ) AS token_address,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
FROM
    node_call
