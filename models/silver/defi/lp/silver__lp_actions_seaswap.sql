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
        *
    FROM
        {{ ref('silver__dex_metadata_seaswap') }}
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
        ON A.attribute_value = b.pool_address
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
            'mint',
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
        j :from :: STRING AS from_address
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
    CASE
        WHEN A.liquidity_received IS NOT NULL THEN 'add_liquidity'
        WHEN A.liquidity_burned IS NOT NULL THEN 'remove_liquidity'
    END AS lp_action,
    A.contract_address AS pool_address,
    C.pool_name,
    COALESCE(
        A.token1_amount,
        A.token1_returned
    ) AS token1_amount,
    C.token1_denom AS token1_currency,
    COALESCE(
        A.token2_amount,
        A.token2_returned
    ) AS token2_amount,
    C.token2_denom AS token2_currency,
    COALESCE(
        A.liquidity_received,
        A.liquidity_burned
    ) AS lp_token_amount,
    C.lp_token_address,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS lp_actions_seaswap_id,
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
    JOIN rel_contracts C
    ON A.contract_address = C.pool_address
WHERE
    COALESCE(
        A.liquidity_received,
        A.liquidity_burned
    ) IS NOT NULL
    AND b.action IN (
        'mint',
        'burn_from'
    )
