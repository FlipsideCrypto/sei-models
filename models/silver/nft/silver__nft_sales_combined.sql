{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
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
    b.exchange_type,
    b.nft_address,
    b.token_id,
    buyer_address,
    seller_address,
    amount,
    currency,
    marketplace_contract,
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
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
