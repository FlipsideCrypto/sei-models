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
        label ILIKE 'Fuzio%'
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
            'add_liquidity',
            'mint',
            'remove_liquidity',
            'burn_from'
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
        j :token1_amount :: INT AS token1_amount,
        j :token2_amount :: INT AS token2_amount,
        j :token1_returned :: INT AS token1_returned,
        j :token2_returned :: INT AS token2_returned,
        j :liquidity_received :: INT AS liquidity_received,
        j :liquidity_burned :: INT AS liquidity_burned,
        j :to :: STRING AS to_address,
        j :from :: STRING AS from_address,
        j :by :: STRING AS by_address,
        j :amount :: INT AS amount
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
        b.to_address,
        b.from_address
    ) AS liquidity_provider_address,
    A.action AS lp_action,
    A.contract_address AS pool_address,
    C.pool_name,
    COALESCE(
        A.token1_amount,
        A.token1_returned
    ) AS token1_amount,
    d.token1_currency,
    COALESCE(
        A.token2_amount,
        A.token2_returned
    ) AS token2_amount,
    d.token2_currency,
    COALESCE(
        A.liquidity_received,
        A.liquidity_burned
    ) AS lp_token_amount,
    d.lp_token_address,
    A._inserted_timestamp
FROM
    wasm A
    JOIN wasm b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
    JOIN rel_contracts C
    ON A.contract_address = C.contract_address
    JOIN contract_info d
    ON A.contract_address = d.contract_address
WHERE
    A.action IN (
        'add_liquidity',
        'remove_liquidity'
    )
    AND b.action IN (
        'mint',
        'burn_from'
    )
