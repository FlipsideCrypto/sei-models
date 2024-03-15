{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
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
    platform_address,
    platform_name,
    nft_sales_combined_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_combined') }}
