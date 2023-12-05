{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'tx',
            'transfer'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :recipient :: STRING AS recipient,
        j :amount :: STRING AS amount
    FROM
        base_atts
    WHERE
        msg_type = 'transfer'
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        _inserted_timestamp
),
sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        base_atts
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
new_fin AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        A._inserted_timestamp,
        COALESCE(
            A.sender,
            s.sender
        ) AS sender,
        A.recipient AS receiver,
        A.amount,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    A.amount,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount_INT,
        RIGHT(A.amount, LENGTH(A.amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(A.amount, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        all_transfers A
        JOIN sender s
        ON A.tx_id = s.tx_id
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    'SEI' AS transfer_type,
    msg_index,
    sender,
    receiver AS receiver,
    amount_int :: INT AS amount,
    currency,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_fin
WHERE
    amount IS NOT NULL
