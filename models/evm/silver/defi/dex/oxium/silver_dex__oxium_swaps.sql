{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH vault_swaps AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS aggregation_router,
        pool_address,
        p.platform,
        p.protocol,
        p.version,
        p.type,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__oxium_pools') }}
        p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x7e7ef152d8e06f2067bb86a7fc9dd47f22285786abd78774843a0cbe47aaca6a' --Swap
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
aggregator_router_swaps AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS token_in_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token_out_address,
        TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [2] :: STRING
                )
            ) AS amount_in_unadj,
        TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [3] :: STRING
                )
            ) AS amount_out_unadj,
        'Swapped' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address IN (
            SELECT 
                aggregation_router
            FROM 
                vault_swaps
        )
        topics [0] :: STRING = '0x384d8f89ff96881986274bc0c1a0bbe8d85b10cf8b0c161dfe37fef72d277e28' --Swap
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
SELECT
    v.block_number,
    v.block_timestamp,
    v.tx_hash,
    v.event_index,
    v.event_name,
    v.origin_function_signature,
    v.origin_from_address,
    v.origin_to_address,
    v.contract_address,
    v.pool_address,
    v.origin_from_address AS recipient,
    v.origin_from_address AS sender,
    a.token_in_address,
    a.token_out_address,
    a.amount_in_unadj,
    a.amount_out_unadj,
    v.platform,
    v.protocol,
    v.version,
    v.type,
    v._log_id,
    v.modified_timestamp
FROM
    vault_swaps v 
    INNER JOIN aggregator_router_swaps a
    ON v.block_number = a.block_number
    AND v.tx_hash = a.tx_hash
    qualify(ROW_NUMBER() over(PARTITION BY v._log_id
ORDER BY
    v.modified_timestamp DESC)) = 1
