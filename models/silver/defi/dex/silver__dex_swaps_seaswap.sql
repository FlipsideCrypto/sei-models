{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    tags = ['noncore','recent_test'],
    enabled = false
) }}

WITH rel_contracts AS (

    SELECT
        pool_address AS contract_address,
        pool_name
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
        {{ ref('silver__msg_attributes') }} A {# WHERE
        block_timestamp :: DATE = '2023-08-24' #}

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
        tx_id
    FROM
        all_txns A
        JOIN rel_contracts b
        ON A.attribute_value = b.contract_address
    WHERE
        msg_type = 'execute'
        AND attribute_key = '_contract_address' {#
    INTERSECT
    SELECT
        tx_id
    FROM
        all_txns A
    WHERE
        msg_type = 'wasm'
        AND attribute_key = 'action'
        AND attribute_value = 'transfer' #}
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
        j :_contract_address :: STRING AS contract_address,
        j :native_sold :: INT AS native_sold,
        j :token_bought :: INT AS token_bought,
        j :action :: STRING AS action,
        j :amount :: INT AS amount,
        j :from :: STRING AS x_from,
        j :to :: STRING AS x_to
    FROM
        all_txns A
        JOIN fee_payer b
        ON A.tx_id = b.tx_id
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
),
xfers AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        tx_fee_payer,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS recipient,
        j :amount :: STRING AS amount_raw,
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
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        all_txns A
        JOIN fee_payer b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'transfer'
    GROUP BY
        A.block_id,
        A.block_timestamp,
        A.tx_succeeded,
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        tx_fee_payer,
        _inserted_timestamp
),
combo AS (
    SELECT
        A.*,
        b.tx_fee_payer,
        x_in.amount AS amount_in,
        x_in.currency AS currency_in,
        COALESCE(
            x_out.amount,
            wasm_out.amount
        ) AS amount_out,
        COALESCE(
            x_out.currency,
            wasm_out.contract_address
        ) AS currency_out {# ,
        x_fee.amount AS amount_fee,
        x_fee.currency AS currency_fee #}
    FROM
        wasm A
        JOIN fee_payer b
        ON A.tx_id = b.tx_id
        JOIN xfers x_in
        ON A.tx_id = x_in.tx_id
        AND A.msg_group = x_in.msg_group
        AND A.msg_sub_group = x_in.msg_sub_group
        AND A.native_sold = x_in.amount
        AND b.tx_fee_payer = x_in.sender
        LEFT JOIN xfers x_out
        ON A.tx_id = x_out.tx_id
        AND A.msg_group = x_out.msg_group
        AND A.msg_sub_group = x_out.msg_sub_group
        AND A.token_bought = x_out.amount
        AND b.tx_fee_payer = x_out.recipient
        LEFT JOIN wasm wasm_out
        ON wasm_out.action = 'transfer'
        AND A.tx_id = wasm_out.tx_id
        AND A.msg_group = wasm_out.msg_group
        AND A.msg_sub_group = wasm_out.msg_sub_group
        AND A.token_bought = wasm_out.amount
        AND b.tx_fee_payer = wasm_out.x_to {# JOIN xfers x_fee
        ON A.tx_id = x_fee.tx_id
        AND A.msg_group = x_fee.msg_group
        AND A.msg_sub_group = x_fee.msg_sub_group
        AND x_in.tx_id IS NULL
        AND x_out.tx_id IS NULL #}
    WHERE
        A.native_sold IS NOT NULL
        AND COALESCE(
            x_out.amount,
            wasm_out.amount
        ) IS NOT NULL
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.tx_fee_payer AS swapper,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.amount_in,
    A.currency_in,
    A.amount_out,
    A.currency_out,
    {# A.amount_fee,
    A.currency_fee,
    #}
    A.contract_address AS pool_address,
    b.pool_name,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS dex_swaps_seaswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combo A
    LEFT JOIN rel_contracts b
    ON A.contract_address = b.contract_address
