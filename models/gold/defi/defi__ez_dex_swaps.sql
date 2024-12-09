{{ config(
    materialized = 'incremental',
    unique_key = 'ez_dex_swaps_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','originated_from','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,origin_from_address,swapper,token_in,token_out,symbol_in,symbol_out);",
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['noncore','recent_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    originated_from,
    platform,
    swapper,
    origin_from_address,
    pool_address,
    amount_in_unadj,
    amount_in,
    amount_in_usd,
    amount_out_unadj,
    amount_out,
    amount_out_usd,
    token_in,
    symbol_in,
    token_out,
    symbol_out,
    origin_function_signature,
    INDEX,
    origin_to_address,
    sender,
    tx_to,
    event_name,
    swaps_combined_id AS ez_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver_evm_dex__swaps_combined') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "30 minutes") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    originated_from,
    platform,
    swapper,
    origin_from_address,
    pool_address,
    amount_in_unadj,
    amount_in,
    amount_in_usd,
    amount_out_unadj,
    amount_out,
    amount_out_usd,
    token_in,
    symbol_in,
    token_out,
    symbol_out,
    origin_function_signature,
    INDEX,
    origin_to_address,
    sender,
    tx_to,
    event_name,
    swaps_combined_id AS ez_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__dex_swaps_combined') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "30 minutes") }}'
        FROM
            {{ this }}
    )
{% endif %}
