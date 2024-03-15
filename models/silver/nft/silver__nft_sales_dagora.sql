{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
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
            'wasm-execute-exchange',
            'wasm' {# ,
            'tx' #}
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
{# sender AS (
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
    attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
),
#}
nft_sales_tx AS (
    SELECT
        tx_id,
        msg_index
    FROM
        msg_atts_base
    WHERE
        msg_type IN(
            'wasm-execute-exchange',
            'wasm'
        )
        AND attribute_key = '_contract_address'
        AND attribute_value = 'sei1pdwlx9h8nc3fp6073mweug654wfkxjaelgkum0a9wtsktwuydw5sduczvz' qualify(ROW_NUMBER() over (PARTITION BY tx_id, msg_index
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
        j :buyer :: STRING AS buyer,
        j :seller :: STRING AS seller,
        j :"is-native-token" :: BOOLEAN AS is_native_token,
        j :_contract_address :: STRING AS marketplace_contract,
        j :amount :: INT AS amount,
        j :token :: STRING AS currency
    FROM
        msg_atts_base A
        INNER JOIN nft_sales_tx s USING (
            tx_id,
            msg_index
        )
    WHERE
        A.msg_type = 'wasm-execute-exchange'
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
        j :action :: STRING AS action,
        j :"exchange-type" :: STRING AS exchange_type,
        j :_contract_address :: STRING AS marketplace_contract,
        j :nft :: STRING AS nft,
        SPLIT_PART(
            j :nft,
            ', ',
            1
        ) AS token_id,
        SPLIT_PART(
            j :nft,
            ', ',
            2
        ) AS nft_address
    FROM
        msg_atts_base A
        INNER JOIN nft_sales_tx s USING (
            tx_id,
            msg_index
        )
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
    HAVING
        action IS NULL
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    b.exchange_type,
    b.nft_address,
    b.token_id,
    A.buyer AS buyer_address,
    A.seller AS seller_address,
    A.amount,
    A.currency,
    A.marketplace_contract,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS nft_sales_dagora_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    nft_sales_buydata A
    LEFT JOIN nft_sales_transfer_data b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
    AND A.marketplace_contract = b.marketplace_contract {# JOIN sender C
    ON A.tx_id = C.tx_id #}
