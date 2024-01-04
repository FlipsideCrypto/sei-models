{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id', 'msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_type,
        msg_index,
        msg_group,
        msg_sub_group,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond',
            'create_validator',
            'tx',
            'coin_spent',
            'message'
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
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        _inserted_timestamp
    FROM
        base A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY A.tx_id
    ORDER BY
        msg_index)) = 1
),
valid AS (
    SELECT
        block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :destination_validator :: STRING
        ) AS validator_address,
        j :source_validator :: STRING AS redelegate_source_validator_address,
        j :amount :: STRING AS amount_raw,
        j :authz_msg_index :: INT AS authz_msg_index,
        j :completion_time :: STRING AS completion_time,
        j :new_shares :: STRING AS new_shares,
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
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        ROW_NUMBER() over (
            PARTITION BY tx_id,
            msg_group,
            msg_sub_group
            ORDER BY
                msg_index DESC
        ) AS del_rank
    FROM
        base A
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond',
            'create_validator'
        )
    GROUP BY
        block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type
),
spent AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :spender :: STRING AS spender,
        j :amount :: STRING AS amount_raw,
        j :authz_msg_index :: INT AS authz_msg_index,
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
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        ROW_NUMBER() over (
            PARTITION BY tx_id,
            msg_group,
            msg_sub_group
            ORDER BY
                msg_index DESC
        ) AS spent_rank
    FROM
        base
    WHERE
        msg_type = 'coin_spent'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
),
spent_auth AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        spender,
        authz_msg_index,
        amount,
        currency
    FROM
        spent
    WHERE
        authz_msg_index IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_id, authz_msg_index
    ORDER BY
        msg_index DESC) = 1)
),
spent_amount AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        spender,
        authz_msg_index,
        amount,
        currency
    FROM
        spent qualify(ROW_NUMBER() over(PARTITION BY tx_id, COALESCE(msg_group, -1), COALESCE(msg_sub_group, -1), amount
    ORDER BY
        msg_index DESC) = 1)
),
wr AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :validator :: STRING AS validator,
        j :amount :: STRING AS amount_raw,
        j :delegator :: STRING AS delegator,
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
        base
    WHERE
        msg_type = 'withdraw_rewards'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
),
sendr AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :module :: STRING AS module,
        j :sender :: STRING AS sender
    FROM
        base
    WHERE
        msg_type = 'message'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
    HAVING
        module = 'staking'
)
SELECT
    block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    b.tx_caller_address,
    REPLACE(
        REPLACE(
            msg_type,
            'unbond',
            'undelegate'
        ),
        'create_validator',
        'delegate'
    ) AS action,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    COALESCE(
        s.sender,
        C.delegator,
        d_auth.spender,
        d.spender,
        d_amount.spender,
        b.tx_caller_address
    ) AS delegator_address,
    A.amount :: INT AS amount,
    A.currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    A.completion_time :: datetime AS completion_time,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id', 'a.msg_index']
    ) }} AS staking_id,
    staking_id AS _unique_key,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    valid A
    JOIN tx_address b
    ON A.tx_id = b.tx_id
    LEFT JOIN sendr s
    ON A.tx_id = s.tx_id
    AND A.msg_group = s.msg_group
    AND COALESCE(
        A.msg_sub_group,
        -1
    ) = COALESCE(
        s.msg_sub_group,
        -1
    )
    LEFT JOIN wr C
    ON A.tx_id = C.tx_id
    AND A.msg_group = C.msg_group
    AND COALESCE(
        A.msg_sub_group,
        -1
    ) = COALESCE(
        C.msg_sub_group,
        -1
    )
    AND A.validator_address = C.validator
    LEFT JOIN spent_auth d_auth
    ON A.tx_id = d_auth.tx_id
    AND A.msg_group = d_auth.msg_group
    AND COALESCE(
        A.msg_sub_group,
        -1
    ) = COALESCE(
        d_auth.msg_sub_group,
        -1
    )
    AND A.authz_msg_index = d_auth.authz_msg_index
    LEFT JOIN spent d
    ON A.tx_id = d.tx_id
    AND A.msg_group = d.msg_group
    AND COALESCE(
        A.msg_sub_group,
        -1
    ) = COALESCE(
        d.msg_sub_group,
        -1
    )
    AND A.amount = d.amount
    AND A.del_rank = d.spent_rank
    AND A.authz_msg_index IS NULL
    LEFT JOIN spent_amount d_amount
    ON A.tx_id = d_amount.tx_id
    AND A.msg_group = d_amount.msg_group
    AND COALESCE(
        A.msg_sub_group,
        -1
    ) = COALESCE(
        d_amount.msg_sub_group,
        -1
    )
    AND A.amount = d_amount.amount
    AND d_auth.tx_id IS NULL
    AND d.tx_id IS NULL
