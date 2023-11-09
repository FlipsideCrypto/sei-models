{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}
{# select 1 a #}
WITH msg_attributes AS (

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
        msg_type IN(
            'withdraw_rewards',
            'transfer',
            'message',
            'tx'
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
reroll_msg AS (
    SELECT
        tx_id,
        msg_type,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS attributes
    FROM
        msg_attributes
    GROUP BY
        tx_id,
        msg_type,
        msg_group,
        msg_sub_group,
        msg_index
),
flat AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        attributes :amount :: STRING AS amount,
        attributes :validator :: STRING AS validator_address,
        attributes :recipient :: STRING AS delegator_address,
        attributes :recipient :: STRING AS rewards_recipient
    FROM
        reroll_msg
    WHERE
        msg_type IN (
            'withdraw_rewards',
            'transfer'
        )
),
combo AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.amount,
        A.validator_address,
        b.delegator_address,
        b.rewards_recipient
    FROM
        flat A
        JOIN flat b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
        AND A.amount = b.amount
    WHERE
        A.msg_type = 'withdraw_rewards'
        AND b.msg_type = 'transfer'
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
        msg_attributes A
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
        A.msg_group,
        A.msg_sub_group,
        A.delegator_address,
        A.rewards_recipient,
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
                p.tx_id,
                p.msg_group,
                p.msg_sub_group,
                p.delegator_address,
                p.validator_address,
                p.rewards_recipient,
                am.value AS split_amount
            FROM
                combo p,
                LATERAL SPLIT_TO_TABLE(
                    p.amount,
                    ','
                ) am
        ) A
        JOIN (
            SELECT
                DISTINCT tx_id,
                block_id,
                block_timestamp,
                tx_succeeded,
                _inserted_timestamp
            FROM
                msg_attributes
        ) b
        ON A.tx_id = b.tx_id
        JOIN tx_address C
        ON A.tx_id = C.tx_id
    GROUP BY
        b.block_id,
        b.block_timestamp,
        A.tx_id,
        b.tx_succeeded,
        C.tx_caller_address,
        A.msg_group,
        A.msg_sub_group,
        A.delegator_address,
        A.rewards_recipient,
        CASE
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
    A.msg_group,
    A.msg_sub_group,
    A.delegator_address,
    A.rewards_recipient,
    A.amount,
    A.currency,
    A.validator_address,
    'withdraw_rewards' AS action,
    A._inserted_timestamp,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.currency,
        A.delegator_address,
        A.validator_address
    ) AS _unique_key
FROM
    prefinal A
