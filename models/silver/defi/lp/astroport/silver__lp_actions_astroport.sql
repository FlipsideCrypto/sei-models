{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}

WITH rel_contracts AS (

    SELECT
        contract_address,
        label AS pool_name
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        label ILIKE 'Astroport%'
),
contract_info AS (
    SELECT
        contract_address,
        DATA :lp_token_address :: STRING AS lp_token_address,
        DATA :token1_denom :native :: STRING AS token1_currency,
        DATA :token2_denom :native :: STRING AS token2_currency
    FROM
        {{ ref('silver__contract_info') }}
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
        {{ ref('silver__msg_attributes') }} A {# WHERE
        block_timestamp :: DATE = '2023-08-23' #}

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
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.sender AS liquidity_provider_address,
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
    A._inserted_timestamp
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
WHERE
    A.action IN (
        'provide_liquidity',
        'withdraw_liquidity'
    )
    AND b.action IN (
        'mint',
        'burn'
    )
