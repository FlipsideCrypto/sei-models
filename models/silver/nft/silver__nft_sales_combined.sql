{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = [ 'block_timestamp::DATE','modified_timestamp::DATE', ],
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    msg_group,
    msg_sub_group,
    msg_index,
    event_type,
    nft_address,
    token_id,
    buyer_address,
    seller_address,
    amount,
    currency,
    marketplace_contract AS platform_address,
    'pallet' AS platform_name,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS nft_sales_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__nft_sales_pallet') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    msg_group,
    msg_sub_group,
    msg_index,
    event_type,
    nft_address,
    token_id,
    buyer_address,
    seller_address,
    amount,
    currency,
    marketplace_contract AS platform_address,
    'dagora' AS platform_name,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS nft_sales_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__nft_sales_dagora') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
