{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}

WITH all_contacts AS (

    SELECT
        contract_address,
        label
    FROM
        {{ ref('silver__contracts') }}
),
rel_contracts AS (
    SELECT
        contract_address,
        label AS pool_name
    FROM
        all_contacts
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
contract_config AS (
    SELECT
        contract_address,
        DATA :lp_token_contract :: STRING AS lp_token_address
    FROM
        {{ ref('silver__contract_config') }}
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
        AND attribute_value = 'withdraw'
),
wasmtran AS (
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
        j :owner :: STRING AS owner,
        j :amount :: STRING AS amount,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS recipient
    FROM
        all_txns A
        JOIN rel_txns b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
    WHERE
        msg_type IN(
            'wasm',
            'transfer'
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
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.owner AS liquidity_provider_address,
    A.action AS lp_reward_action,
    A.contract_address AS staking_pool_address,
    b.pool_name AS staking_pool_name,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                a_tran.amount,
                '[^[:digit:]]',
                ' '
            )
        ),
        ' ',
        0
    ) AS reward_amount,
    RIGHT(a_tran.amount, LENGTH(a_tran.amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(a_tran.amount, '[^[:digit:]]', ' ')), ' ', 0))) AS reward_currency,
    C.lp_token_address AS pool_address,
    d.label AS pool_name,
    A._inserted_timestamp
FROM
    wasmtran A
    JOIN wasmtran a_tran
    ON A.tx_id = a_tran.tx_id
    AND A.msg_group = a_tran.msg_group
    AND A.msg_sub_group = a_tran.msg_sub_group
    AND A.owner = a_tran.recipient
    AND A.contract_address = a_tran.sender
    JOIN rel_contracts b
    ON A.contract_address = b.contract_address
    JOIN contract_config C
    ON A.contract_address = C.contract_address
    JOIN all_contacts d
    ON C.lp_token_address = d.contract_address
