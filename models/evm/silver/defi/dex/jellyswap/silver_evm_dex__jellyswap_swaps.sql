{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'jellyswap_swaps_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        'Swap' AS event_name,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.contract_address,
        topics[1]::STRING as pool_id,
        p.pool_address,
        CONCAT('0x', SUBSTR(topics[2]::STRING, 27, 40)) as token_in,
        CONCAT('0x', SUBSTR(topics[3]::STRING, 27, 40)) as token_out,
        regexp_substr_all(SUBSTR(data, 3, len(data)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            's2c',
            segmented_data[0]::STRING
        ) as amount_in_raw,
        utils.udf_hex_to_int(
            's2c',
            segmented_data[1]::STRING
        ) as amount_out_raw,
        _inserted_timestamp
    FROM 
        {{ ref('silver_evm__logs') }} l
    JOIN
        {{ ref('silver_evm_dex__jellyswap_pools') }} p
        ON l.topics[1]::STRING = p.pool_id
    WHERE 
        topics[0]::STRING = '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b' -- Swap event
        AND tx_status = 'SUCCESS'

    {% if is_incremental() %}
        AND l.modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '5 minutes'
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_address,
    pool_id,
    token_in,
    token_out,
    amount_in_raw as amount_in_unadj,
    amount_out_raw as amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS jellyswap_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    swaps_base