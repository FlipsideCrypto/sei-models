{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE','lp_action'],
    tags = ['noncore']
) }}

WITH rel_contracts AS (

    SELECT
        contract_address,
        label AS pool_name
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        label ILIKE 'Levana%'
),
contract_info AS (
    SELECT
        contract_address,
        DATA :collateral :native :denom :: STRING AS collateral_denom,
        DATA :config :market_id :: STRING AS market_id
    FROM
        {{ ref('silver__contract_status') }}
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
    WHERE
        msg_type ILIKE 'wasm%lp%'

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
        msg_sub_group,
        msg_index
    FROM
        all_txns A
        JOIN rel_contracts b
        ON A.attribute_value = b.contract_address
    WHERE
        attribute_key = '_contract_address'
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
        A.msg_type,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :_contract_address :: STRING AS contract_address,
        j :addr :: STRING AS addr,
        j :"action-id" :: INT AS action_id,
        j :kind :: STRING AS kind,
        j :collateral :: FLOAT AS collateral,
        j :tokens :: FLOAT AS tokens
    FROM
        all_txns A
        JOIN rel_txns b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A.msg_type,
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
    A.addr AS liquidity_provider_address,
    A.kind AS lp_action,
    A.contract_address AS pool_address,
    COALESCE(
        C.pool_name,
        d.market_id
    ) AS pool_name,
    CASE
        WHEN A.contract_address = 'sei14j7zhcj50qsk6vhu7dsa48r5e7v37nthnwwx0q8q4nd0h39udy6qhqq6dm' THEN A.collateral * pow(
            10,
            6
        )
        WHEN A.contract_address = 'sei1jvl8avv45sj92q9x9c84fq2ymddya6dkwv9euf7y365tkzma38zq5xldpy' THEN A.collateral * pow(
            10,
            8
        )
        ELSE collateral
    END AS token1_amount,
    d.collateral_denom AS token1_currency,
    NULL AS token2_amount,
    NULL AS token2_currency,
    tokens AS lp_token_amount,
    NULL AS lp_token_address,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS lp_actions_levana_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wasm A
    JOIN rel_contracts C
    ON A.contract_address = C.contract_address
    JOIN contract_info d
    ON A.contract_address = d.contract_address
WHERE
    A.kind IS NOT NULL
