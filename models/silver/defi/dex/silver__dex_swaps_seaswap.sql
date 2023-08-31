{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    enabled = false
) }}

WITH rel_contracts AS (

    SELECT
        contract_address,
        label AS pool_name
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        label ILIKE '%seaswap%pool%'
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
        AND attribute_key = '_contract_address' {# INTERSECT
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
        AND attribute_value = 'swap' #}
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
        j :native_sold :: INT AS native_sold,
        j :token_bought :: INT AS token_bought
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
        j :amount :: STRING AS amount
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
        *
    FROM
        wasm A
        JOIN
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
    {# A.sender,
    A.recipient,
    #}
    A.amount_in AS ain_raw,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                amount_in,
                '[^[:digit:]]',
                ' '
            )
        ),
        ' ',
        0
    ) AS amount_in,
    RIGHT(amount_in, LENGTH(amount_in) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_in, '[^[:digit:]]', ' ')), ' ', 0))) AS currency_in,
    A.amount_out aou_raw,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                amount_out,
                '[^[:digit:]]',
                ' '
            )
        ),
        ' ',
        0
    ) AS amount_out,
    RIGHT(amount_out, LENGTH(amount_out) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_out, '[^[:digit:]]', ' ')), ' ', 0))) AS currency_out,
    A.pool_address,
    b.pool_name,
    A._inserted_timestamp
FROM
    fin A
    LEFT JOIN rel_contracts b
    ON A.pool_address = b.contract_address
