{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    tags = ['noncore']
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
        tx_id,
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
        AND attribute_value IN (
            'provide_liquidity',
            'mint',
            'withdraw_liquidity',
            'burn'
        )
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
        j :_contract_address :: STRING AS contract_address,
        j :action :: STRING AS action,
        j :sender :: STRING AS sender,
        j :receiver :: STRING AS receiver,
        j :to :: STRING AS to_address,
        j :assets :: STRING AS assets,
        j :refund_assets :: STRING AS refund_assets,
        j :share :: INT AS SHARE,
        j :withdrawn_share :: INT AS withdrawn_share,
        j :amount :: INT AS amount,
        SPLIT_PART(COALESCE(assets, refund_assets), ',', 1) AS token1_raw,
        SUBSTRING(
            SPLIT_PART(COALESCE(assets, refund_assets), ',', 2),
            2,
            9999
        ) AS token2_raw,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    token1_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS token1_amount,
        RIGHT(token1_raw, LENGTH(token1_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(token1_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS token1_currency,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    token2_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS token2_amount,
        RIGHT(token2_raw, LENGTH(token2_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(token2_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS token2_currency
    FROM
        all_txns A
        JOIN rel_txns b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
    WHERE
        msg_type = 'wasm'
        AND attribute_key <> 'user'
        AND NOT (
            attribute_key = 'action'
            AND attribute_value NOT IN (
                'provide_liquidity',
                'mint',
                'withdraw_liquidity',
                'burn'
            )
        )
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        _inserted_timestamp
    HAVING
        action IN (
            'provide_liquidity',
            'mint',
            'withdraw_liquidity',
            'burn'
        )
),
tx_sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_sender
    FROM
        all_txns
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    COALESCE(
        A.sender,
        A.to_address,
        A.receiver,
        d.tx_sender
    ) AS liquidity_provider_address,
    A.action AS lp_action,
    A.contract_address AS pool_address,
    C.pool_name,
    A.token1_raw,
    A.token1_amount,
    A.token1_currency,
    A.token2_raw,
    A.token2_amount,
    A.token2_currency,
    COALESCE(
        A.share,
        A.withdrawn_share
    ) AS lp_token_amount,
    b.contract_address AS lp_token_address,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS lp_actions_astroport_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wasm A
    JOIN wasm b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
    AND COALESCE(
        A.share,
        A.withdrawn_share
    ) = b.amount
    JOIN rel_contracts C
    ON A.contract_address = C.contract_address
    JOIN tx_sender d
    ON A.tx_id = d.tx_id
WHERE
    A.action IN (
        'provide_liquidity',
        'withdraw_liquidity'
    )
    AND b.action IN (
        'mint',
        'burn'
    )
