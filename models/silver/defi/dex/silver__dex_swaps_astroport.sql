{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    tags = ['noncore','recent_test']
) }}

WITH rel_contracts AS (

    SELECT
        A.contract_address,
        b.label AS pool_name
    FROM
        {{ ref('silver__dex_metadata_astroport') }} A
        LEFT JOIN {{ ref('silver__contracts') }}
        b
        ON A.contract_address = b.contract_address
    WHERE
        A.type = 'pair_contract_addr'
),
all_txns AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_type,
        msg_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
rel_txns AS (
    SELECT
        DISTINCT tx_id,
        block_timestamp,
        msg_group,
        msg_sub_group
    FROM
        all_txns A
        JOIN rel_contracts b
        ON A.attribute_value = b.contract_address
    WHERE
        msg_type = 'execute'
        AND attribute_key = '_contract_address'
    INTERSECT
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        msg_sub_group
    FROM
        all_txns A
    WHERE
        msg_type = 'wasm'
        AND attribute_key = 'action'
        AND attribute_value = 'swap'
),
fee_payer AS (
    SELECT
        A.tx_id,
        attribute_value AS tx_fee_payer
    FROM
        all_txns A
        JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                rel_txns
        ) b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'fee_payer'
),
wasm AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :_contract_address :: STRING AS _contract_address,
        {# j :action :: STRING AS action, #}
        j :ask_asset :: STRING AS ask_asset,
        j :commission_amount :: INT AS commission_amount,
        j :maker_fee_amount :: INT AS maker_fee_amount,
        j :offer_amount :: INT AS offer_amount,
        j :offer_asset :: STRING AS offer_asset,
        j :receiver :: STRING AS receiver,
        j :return_amount :: INT AS return_amount,
        j :sender :: STRING AS sender,
        j :spread_amount :: INT AS spread_amount,
        j :tax_amount :: INT AS tax_amount
    FROM
        all_txns A
        JOIN rel_txns b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
    WHERE
        msg_type = 'wasm'
        AND attribute_key <> 'action'
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        _inserted_timestamp {# HAVING
        action = 'swap' #}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    b.tx_fee_payer swapper,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    offer_amount AS amount_in,
    offer_asset AS currency_in,
    return_amount AS amount_out,
    ask_asset AS currency_out,
    commission_amount,
    maker_fee_amount,
    spread_amount,
    _contract_address AS pool_address,
    C.pool_name,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS dex_swaps_astroport_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wasm A
    JOIN fee_payer b
    ON A.tx_id = b.tx_id
    JOIN rel_contracts C
    ON A._contract_address = C.contract_address
