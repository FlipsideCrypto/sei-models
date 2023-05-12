{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

select 1 a
{# 
WITH msg_attributes_cte AS (

    SELECT
        tx_id,
        msg_type,
        msg_group,
        msg_sub_group,
        msg_index,
        attribute_key,
        attribute_value,
        block_id,
        block_timestamp,
        tx_succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type IN (
            'withdraw_rewards',
            'transfer',
            'message',
            'tx',
            'delegate',
            'redelegate',
            'unbond'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
reward_combo AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :validator :: STRING AS validator_address,
        j :amount :: STRING AS amount,
        j :sender :: STRING AS delegator_address
    FROM
        msg_attr_rewards
    WHERE
        attribute_key IN (
            'sender',
            'amount',
            'validator'
        )
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group
),
claim_base AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group
    FROM
        msg_attributes_cte A
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                msg_attributes_cte
            WHERE
                msg_type IN (
                    'delegate',
                    'redelegate'
                )
        ) b
        ON A.tx_id = b.tx_id
    WHERE
        msg_type = 'claim'
),
claim_combo AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        OBJECT_AGG(
            CASE
                WHEN msg_type IN (
                    'message',
                    'claim'
                ) THEN msg_type || '__'
                ELSE ''
            END || attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :source_validator :: STRING
        ) AS validator_address,
        j :claim__amount :: STRING AS amount,
        j :message__sender :: STRING AS delegator_address
    FROM
        msg_attr_claim
    WHERE
        msg_type || '__' || attribute_key IN (
            'message__sender',
            'claim__amount'
        )
        OR attribute_key IN (
            'validator',
            'source_validator'
        )
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group
),
tran_base AS (
    SELECT
        DISTINCT tx_id,
        msg_group,
        msg_sub_group,
        delegator_address
    FROM
        {{ ref('silver__staking') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}
),
tran_tran AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        A.msg_group,
        attribute_key,
        attribute_value
    FROM
        msg_attributes_cte A
        INNER JOIN tran_base b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.attribute_value = b.delegator_address
        LEFT JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                msg_attributes_cte
            WHERE
                msg_type IN (
                    'claim',
                    'withdraw_rewards'
                )
        ) C
        ON A.tx_id = C.tx_id
    WHERE
        A.msg_type = 'transfer'
        AND A.attribute_key = 'recipient'
        AND C.tx_id IS NULL
),
msg_attr_trans AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index,
        A.msg_type,
        A.attribute_key,
        A.attribute_value,
        DENSE_RANK() over(
            PARTITION BY A.tx_id,
            A.msg_group
            ORDER BY
                A.msg_index
        ) change_index
    FROM
        tran_tran b
        JOIN msg_attributes_cte A
        ON A.tx_ID = b.tx_ID
        AND A.msg_group = b.msg_group
    WHERE
        A.msg_type IN (
            'delegate',
            'redelegate',
            'unbond'
        )
        OR (
            A.msg_index = b.msg_index
            AND A.msg_type = 'transfer'
        )
),
tran_combo AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_index group_id,
        COALESCE(
            b.j :validator :: STRING,
            b.j :source_validator :: STRING
        ) AS validator_address,
        A.j :amount :: STRING AS amount,
        A.j :recipient :: STRING AS delegator_address
    FROM
        (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index + 1 group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type = 'transfer'
                        AND attribute_key IN (
                            'amount',
                            'recipient'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) A
        JOIN (
            SELECT
                tx_id,
                msg_group,
                msg_index,
                change_index group_id,
                OBJECT_AGG(
                    attribute_key :: STRING,
                    attribute_value :: variant
                ) AS j
            FROM
                (
                    SELECT
                        DISTINCT tx_id,
                        msg_group,
                        msg_index,
                        change_index,
                        attribute_key,
                        attribute_value
                    FROM
                        msg_attr_trans
                    WHERE
                        msg_type <> 'transfer'
                        AND attribute_key IN (
                            'validator',
                            'source_validator'
                        )
                ) x
            GROUP BY
                tx_id,
                msg_group,
                msg_index,
                group_id
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.group_id = b.group_id
),
combo_all AS (
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'claim' AS action
    FROM
        tran_combo
    UNION ALL
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'withdraw_rewards' AS action
    FROM
        reward_combo
    UNION ALL
    SELECT
        tx_id,
        msg_group,
        group_id,
        validator_address,
        amount,
        delegator_address,
        'claim' AS action
    FROM
        claim_combo
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        SPLIT_PART(
            attribute_value,
            '/',
            1
        ) AS acc_seq_index
    FROM
        msg_attributes_cte A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        acc_seq_index) = 1)
),
prefinal AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        A.tx_id,
        b.tx_succeeded,
        C.tx_caller_address,
        A.action,
        A.msg_group,
        A.delegator_address,
        SUM(
            CASE
                WHEN A.split_amount LIKE '%usei' THEN REPLACE(
                    A.split_amount,
                    'usei'
                )
                WHEN A.split_amount LIKE '%pool%' THEN LEFT(A.split_amount, CHARINDEX('g', A.split_amount) -1)
                WHEN A.split_amount LIKE '%ibc%' THEN LEFT(A.split_amount, CHARINDEX('i', A.split_amount) -1)
                ELSE A.split_amount
            END :: INT
        ) AS amount,
        CASE
            WHEN A.split_amount LIKE '%usei' THEN 'usei'
            WHEN A.split_amount LIKE '%pool%' THEN SUBSTRING(A.split_amount, CHARINDEX('g', A.split_amount), 99)
            WHEN A.split_amount LIKE '%ibc%' THEN SUBSTRING(A.split_amount, CHARINDEX('i', A.split_amount), 99)
            ELSE 'usei'
        END AS currency,
        A.validator_address,
        b._inserted_timestamp
    FROM
        (
            SELECT
                p.tx_Id,
                p.action,
                p.msg_group,
                p.delegator_address,
                p.validator_address,
                am.value AS split_amount
            FROM
                combo_all p,
                LATERAL SPLIT_TO_TABLE(
                    p.amount,
                    ','
                ) am
        ) A
        JOIN (
            SELECT
                tx_ID,
                block_id,
                block_timestamp,
                tx_succeeded,
                _inserted_timestamp
            FROM
                {{ ref('silver__transactions') }} A

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}
) b
ON A.tx_Id = b.tx_ID
JOIN tx_address C
ON A.tx_id = C.tx_id
GROUP BY
    b.block_id,
    b.block_timestamp,
    A.tx_id,
    b.tx_succeeded,
    C.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,CASE
        WHEN A.split_amount LIKE '%usei' THEN 'usei'
        WHEN A.split_amount LIKE '%pool%' THEN SUBSTRING(A.split_amount, CHARINDEX('g', A.split_amount), 99)
        WHEN A.split_amount LIKE '%ibc%' THEN SUBSTRING(A.split_amount, CHARINDEX('i', A.split_amount), 99)
        ELSE 'usei'
    END,
    A.validator_address,
    b._inserted_timestamp
)
SELECT
    block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.tx_caller_address,
    A.action,
    A.msg_group,
    A.delegator_address,
    A.amount,
    A.currency,
    A.validator_address,
    A._inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        A.action,
        A.currency,
        A.delegator_address,
        A.validator_address
    ) AS _unique_key
FROM
    prefinal A #}
