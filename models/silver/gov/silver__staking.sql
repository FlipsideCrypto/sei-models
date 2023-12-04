{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH pre_base AS (

    SELECT
        tx_id,
        msg_type,
        msg_index,
        msg_group,
        msg_sub_group,
        attribute_key,
        attribute_value,
        block_id,
        block_timestamp,
        tx_succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        (
            msg_type IN (
                'delegate',
                'redelegate',
                'unbond',
                'create_validator',
                'message'
            )
            OR attribute_key = 'acc_seq'
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
base AS (
    SELECT
        A.tx_id,
        A.msg_type,
        A.msg_index,
        msg_group,
        msg_sub_group
    FROM
        pre_base A
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond',
            'create_validator'
        )
),
msg_attr AS (
    SELECT
        A.tx_id,
        A.attribute_key,
        A.attribute_value,
        A.msg_index,
        A.msg_type,
        A.msg_group,
        A.msg_sub_group,
        block_id,
        block_timestamp,
        tx_succeeded,
        _inserted_timestamp
    FROM
        pre_base A
        JOIN (
            SELECT
                DISTINCT tx_id,
                msg_index
            FROM
                base
            UNION ALL
            SELECT
                DISTINCT tx_id,
                msg_index + 1 msg_index
            FROM
                base
        ) b
        ON A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    WHERE
        A.msg_type IN (
            'delegate',
            'message',
            'redelegate',
            'unbond',
            'create_validator'
        )
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address
    FROM
        pre_base A
        JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                base
        ) b
        ON A.tx_id = b.tx_id
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY A.tx_id
    ORDER BY
        msg_index)) = 1
),
valid AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        COALESCE(
            j :validator :: STRING,
            j :destination_validator :: STRING
        ) AS validator_address,
        j :source_validator :: STRING AS redelegate_source_validator_address
    FROM
        msg_attr
    WHERE
        attribute_key LIKE '%validator'
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
        j :sender :: STRING AS sender
    FROM
        msg_attr A
    WHERE
        attribute_key = 'sender'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
),
amount AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :amount :: STRING AS amount
    FROM
        msg_attr
    WHERE
        attribute_key = 'amount'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
),
ctime AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :completion_time :: STRING AS completion_time
    FROM
        msg_attr
    WHERE
        attribute_key = 'completion_time'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
),
prefinal AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        b.sender AS delegator_address,
        d.amount,
        A.msg_type AS action,
        C.validator_address,
        C.redelegate_source_validator_address,
        e.completion_time
    FROM
        (
            SELECT
                DISTINCT tx_id,
                msg_group,
                msg_sub_group,
                msg_index,
                REPLACE(
                    REPLACE(
                        msg_type,
                        'unbond',
                        'undelegate'
                    ),
                    'create_validator',
                    'delegate'
                ) msg_type
            FROM
                base
        ) A
        JOIN sendr b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_index + 1 = b.msg_index
        JOIN valid C
        ON A.tx_id = C.tx_id
        AND A.msg_group = C.msg_group
        AND A.msg_index = C.msg_index
        JOIN amount d
        ON A.tx_id = d.tx_id
        AND A.msg_group = d.msg_group
        AND A.msg_index = d.msg_index
        LEFT JOIN ctime e
        ON A.tx_id = e.tx_id
        AND A.msg_group = e.msg_group
        AND A.msg_index = e.msg_index
),
add_dec AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        A.tx_id,
        b.tx_succeeded,
        C.tx_caller_address,
        A.action,
        A.msg_group,
        A.msg_sub_group,
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
        A.redelegate_source_validator_address,
        A.completion_time :: datetime completion_time,
        b._inserted_timestamp
    FROM
        (
            SELECT
                p.tx_id,
                p.action,
                p.msg_group,
                p.msg_sub_group,
                p.delegator_address,
                p.validator_address,
                p.redelegate_source_validator_address,
                p.completion_time,
                am.value AS split_amount
            FROM
                prefinal p,
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
                msg_attr
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
        A.action,
        A.msg_group,
        A.msg_sub_group,
        A.delegator_address,
        currency,
        A.validator_address,
        A.redelegate_source_validator_address,
        completion_time,
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
    A.msg_sub_group,
    A.delegator_address,
    A.amount,
    A.currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    A.completion_time,
    concat_ws(
        '-',
        tx_id,
        msg_group,
        COALESCE(
            msg_sub_group,
            -1
        ),
        action,
        currency,
        delegator_address,
        validator_address
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    add_dec A
