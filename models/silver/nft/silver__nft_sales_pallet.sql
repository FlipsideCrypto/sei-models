{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE' ],
    tags = ['noncore']
) }}

WITH msg_atts_base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        _inserted_timestamp,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        tx_succeeded
        AND msg_type IN (
            'wasm-buy_now',
            'wasm-accept_bid',
            'wasm',
            'tx'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= GREATEST(
    (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE -3
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id, msg_index, attribute_key
ORDER BY
    attribute_index) = 1) -- this is needed to handle the case where the same attribute key "collection_address" is repeated in the same message
),
sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_from
    FROM
        msg_atts_base
    WHERE
        (
            (
                msg_type = 'tx'
                AND attribute_key = 'acc_seq'
            )
            OR (
                msg_type = 'signer'
                AND attribute_key = 'sei_addr'
            )
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
nft_sales_tx AS (
    SELECT
        tx_id
    FROM
        msg_atts_base
    WHERE
        -- BUYS on Pallet Exchange contract: sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9
        msg_type IN (
            'wasm-buy_now',
            'wasm-accept_bid'
        )
        AND attribute_key = '_contract_address'
        AND attribute_value = 'sei152u2u0lqc27428cuf8dx48k8saua74m6nql5kgvsu4rfeqm547rsnhy4y9' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
nft_sales_buydata AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.msg_type,
        A._inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :nft_address :: STRING AS nft_address,
        j: collection_address :: STRING AS collection_address,
        j :nft_token_id :: STRING AS nft_token_id,
        j :token_id :: STRING AS token_id,
        j :nft_seller :: STRING AS nft_seller,
        j :buyer :: STRING AS buyer,
        j :seller :: STRING AS seller,
        j :sold_to :: STRING AS nft_sold_to,
        j :bidder :: STRING AS bidder,
        j :_contract_address :: STRING AS marketplace_contract,
        j :sale_price :: STRING AS sale_price,
        REPLACE(
            REPLACE(
                j :sale_price,
                'native:'
            ),
            'usei:'
        ) :: STRING AS amount_raw,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    amount_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) :: INT AS amount,
        CASE
            WHEN sale_price LIKE '%usei%' THEN 'usei'
            ELSE RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0)))
        END AS currency
    FROM
        msg_atts_base A
        INNER JOIN nft_sales_tx s USING (tx_id)
    WHERE
        A.msg_type IN (
            'wasm-buy_now',
            'wasm-accept_bid'
        )
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.msg_type,
        A._inserted_timestamp
),
nft_sales_transfer_data AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A._inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS marketplace_contract,
        j :token_id :: STRING AS token_id,
        j :recipient :: STRING AS nft_buyer,
        j :buyer :: STRING AS buyer,
        j :seller :: STRING AS seller,
        j :bidder :: STRING AS bidder,
        j :_contract_address :: STRING AS nft_address
    FROM
        msg_atts_base A
        INNER JOIN nft_sales_tx s USING (tx_id)
    WHERE
        msg_type = 'wasm'
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A._inserted_timestamp
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    CASE
        A.msg_type
        WHEN 'wasm-buy_now' THEN 'sale'
        WHEN 'wasm-accept_bid' THEN 'bid_won'
    END AS event_type,
    COALESCE(
        A.nft_address,
        A.collection_address
    ) AS nft_address,
    COALESCE(
        A.nft_token_id,
        A.token_id
    ) AS token_id,
    COALESCE(
        b.nft_buyer,
        b.buyer,
        A.nft_sold_to,
        A.buyer,
        A.bidder,
        b.bidder,
        C.tx_from
    ) AS buyer_address,
    COALESCE(
        A.nft_seller,
        A.seller,
        C.tx_from
    ) AS seller_address,
    A.amount :: INT AS amount,
    A.currency,
    A.marketplace_contract,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS nft_sales_pallet_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    nft_sales_buydata A
    LEFT JOIN nft_sales_transfer_data b
    ON A.tx_id = b.tx_id
    AND A.nft_address = b.nft_address
    AND A.token_id = b.token_id
    AND A.marketplace_contract = b.marketplace_contract
    JOIN sender C
    ON A.tx_id = C.tx_id
