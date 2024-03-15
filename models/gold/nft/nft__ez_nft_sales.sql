{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_number,
        version,
        tx_hash,
        event_index,
        event_type,
        buyer_address,
        seller_address,
        nft_address,
        token_version,
        platform_address,
        project_name,
        tokenid,
        platform_name,
        platform_exchange_version,
        total_price_raw,
        total_price_raw / pow(
            10,
            b.decimals
        ) AS total_price,
        total_price * C.price AS total_price_usd,
        platform_fee_raw / pow(
            10,
            b.decimals
        ) AS platform_fee,
        platform_fee * C.price AS platform_fee_usd,
        creator_fee_raw / pow(
            10,
            b.decimals
        ) AS creator_fee,
        creator_fee * C.price AS creator_fee_usd,
        total_fees_raw / pow(
            10,
            b.decimals
        ) AS total_fees,
        total_fees * C.price AS total_fees_usd,
        aggregator_name,
        currency_address,
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
        C.price
    FROM
        {{ ref('nft__fact_nft_sales') }} A
        LEFT JOIN {{ ref('core__dim_tokens') }}
        b
        ON A.currency_address = b.token_address
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }} C
        ON LOWER(
            A.currency_address
        ) = C.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = C.hour
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    buyer_address,
    seller_address,
    nft_address,
    token_version,
    platform_address,
    project_name,
    tokenid,
    platform_name,
    platform_exchange_version,
    total_price,
    total_price_usd,
    platform_fee,
    platform_fee_usd,
    creator_fee,
    creator_fee_usd,
    total_fees,
    total_fees_usd,
    aggregator_name,
    currency_address,
    ez_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
