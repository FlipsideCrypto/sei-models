{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    enabled = true,
    tags = ['noncore']
) }}

WITH rel_contracts AS (

    SELECT
        contract_address,
        label AS pool_name
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        label ILIKE 'Fuzio%Pool%'
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
        msg_type IN (
            'wasm',
            'transfer',
            'tx'
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
        msg_type = 'wasm'
        AND attribute_key = '_contract_address'
    INTERSECT
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        msg_sub_group,
        msg_index
    FROM
        all_txns A
    WHERE
        msg_type = 'wasm'
        AND (
            (
                attribute_key = 'action'
                AND attribute_value = 'swap'
            )
            OR attribute_key = 'native_transferred'
        )
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
        j :action :: STRING AS action,
        COALESCE(
            j :native_sold :: INT,
            j :input_token_amount :: INT
        ) AS amount_in,
        COALESCE(
            j :token_bought :: INT,
            j :native_transferred :: INT
        ) AS amount_out
    FROM
        all_txns A
        JOIN rel_txns b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
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
xfer AS (
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
        ) AS amount,
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        all_txns A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_group,
                msg_sub_group
            FROM
                rel_txns
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
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
        _inserted_timestamp qualify(ROW_NUMBER() over(PARTITION BY A.tx_id, A.msg_group, A.msg_sub_group, amount
    ORDER BY
        A.msg_index) = 1)
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    fp.tx_fee_payer AS swapper,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.amount_in,
    C.currency AS currency_in,
    A.amount_out,
    d.currency AS currency_out,
    A.contract_address AS pool_address,
    b.pool_name,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.msg_index']
    ) }} AS dex_swaps_fuzio_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wasm A
    JOIN fee_payer fp
    ON A.tx_id = fp.tx_id
    JOIN rel_contracts b
    ON A.contract_address = b.contract_address
    LEFT JOIN xfer C
    ON A.tx_id = C.tx_id
    AND A.msg_group = C.msg_group
    AND A.msg_sub_group = C.msg_sub_group
    AND A.amount_in = C.amount
    AND A.msg_index > C.msg_index
    LEFT JOIN xfer d
    ON A.tx_id = d.tx_id
    AND A.msg_group = d.msg_group
    AND A.msg_sub_group = d.msg_sub_group
    AND A.amount_out = d.amount
    AND A.msg_index < d.msg_index
