{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        event_type,
        buyer_address,
        seller_address,
        nft_address,
        platform_address,
        ccin.data :data :name :: STRING AS project_name,
        token_id,
        platform_name,
        amount_raw,
        amount_raw / pow(
            10,
            b.decimals
        ) AS amount,
        amount * p.close AS amount_usd,
        A.currency,
        fact_nft_sales_id AS ez_nft_sales_id,
        GREATEST(
            A.inserted_timestamp,
            b.inserted_timestamp
        ) AS inserted_timestamp,
        GREATEST(
            A.modified_timestamp,
            b.modified_timestamp
        ) AS modified_timestamp,
        b.decimals,
        b.symbol,
        p.close AS price
    FROM
        {{ ref('nft__fact_nft_sales') }} A
        LEFT JOIN {{ ref('core__dim_tokens') }}
        b
        ON A.currency = b.currency
        LEFT JOIN {{ ref('silver__hourly_prices_coin_gecko') }}
        p
        ON DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = p.recorded_hour
        AND p.id = 'sei-network'
        LEFT JOIN {{ ref('silver__contract_contract_info') }}
        ccin
        ON A.nft_address = ccin.contract_address
)
SELECT
    block_timestamp,
    block_id,
    tx_succeeded,
    tx_id,
    msg_group,
    msg_sub_group,
    msg_index,
    event_type,
    buyer_address,
    seller_address,
    nft_address,
    token_id,
    platform_address,
    platform_name,
    amount,
    amount_usd,
    currency,
    ez_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
