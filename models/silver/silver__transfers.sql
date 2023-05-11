{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
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
        (
            attribute_key IN (
                'acc_seq',
                'amount'
            )
            OR msg_type IN (
                'coin_spent',
                'transfer',
                'message',
                'claim',
                'ibc_transfer',
                'write_acknowledgement'
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
) - INTERVAL '48 HOURS'
{% endif %}
),
sender AS (
    SELECT
        tx_id,
        msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        base_atts
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
message_index_ibc AS (
    SELECT
        att.tx_id,
        msg_group,
        msg_sub_group,
        MAX(
            att.msg_index
        ) AS max_index
    FROM
        base_atts att
        INNER JOIN sender s
        ON att.tx_id = s.tx_id
    WHERE
        (
            msg_type = 'coin_spent'
            OR msg_type = 'transfer'
        )
        AND attribute_key = 'amount'
        AND att.msg_index > s.msg_index
        AND msg_group IS NOT NULL
    GROUP BY
        att.tx_id,
        msg_group,
        msg_sub_group
),
coin_sent_ibc AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        attribute_value,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(attribute_value) :amount
        ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(attribute_value) [1] :denom
        ) AS currency
    FROM
        base_atts A
        LEFT OUTER JOIN message_index_ibc m
        ON A.tx_id = m.tx_id
        AND A.msg_group = m.msg_group
        AND A.msg_sub_group = m.msg_sub_group
    WHERE
        A.msg_index = m.max_index
        AND A.attribute_key = 'amount'
),
receiver_ibc AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        COALESCE(
            attribute_value,
            TRY_PARSE_JSON(attribute_value) :receiver
        ) AS receiver,
        MAX(msg_index) AS msg_index
    FROM
        base_atts
    WHERE
        msg_type = 'ibc_transfer'
        AND attribute_key = 'receiver'
    GROUP BY
        tx_id,
        receiver,
        msg_group,
        msg_sub_group
),
osmo_tx_ids AS (
    SELECT
        DISTINCT tx_id
    FROM
        base_atts
    WHERE
        (
            msg_type = 'message'
            AND attribute_key = 'module'
            AND attribute_value = 'bank'
        )
        OR msg_type = 'claim'
),
message_indexes_osmo AS (
    SELECT
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        osmo_tx_ids v
        LEFT OUTER JOIN base_atts m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index
),
osmo_receiver AS (
    SELECT
        o.tx_id,
        m.msg_group,
        m.msg_index,
        attribute_value AS receiver
    FROM
        osmo_tx_ids o
        LEFT OUTER JOIN base_atts m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes_osmo idx
        ON idx.tx_id = m.tx_id
    WHERE
        m.msg_type = 'transfer'
        AND m.attribute_key = 'recipient'
        AND idx.msg_index = m.msg_index
),
osmo_amount AS (
    SELECT
        o.tx_id,
        m.msg_index,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        osmo_tx_ids o
        LEFT OUTER JOIN base_atts m
        ON o.tx_id = m.tx_id
        LEFT OUTER JOIN message_indexes_osmo idx
        ON idx.tx_id = m.tx_id
    WHERE
        m.msg_type = 'transfer'
        AND m.attribute_key = 'amount'
        AND idx.msg_index = m.msg_index
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        r.tx_id,
        t.tx_succeeded,
        'IBC_TRANSFER_OUT' AS transfer_type,
        r.msg_index,
        sender,
        COALESCE(
            c_old.amount,
            C.amount
        ) AS amount,
        COALESCE(
            c_old.currency,
            C.currency
        ) AS currency,
        receiver,
        _inserted_timestamp,
        concat_ws(
            '-',
            r.tx_id,
            r.msg_index,
            COALESCE(
                c_old.currency,
                C.currency
            )
        ) AS _unique_key
    FROM
        receiver_ibc r
        LEFT OUTER JOIN coin_sent_ibc C
        ON r.tx_id = C.tx_id
        AND r.msg_group = C.msg_group
        AND r.msg_sub_group = C.msg_sub_group + 1
        LEFT OUTER JOIN coin_sent_ibc c_old
        ON r.tx_id = c_old.tx_id
        AND r.msg_group = c_old.msg_group
        AND r.msg_sub_group = c_old.msg_sub_group
        LEFT OUTER JOIN sender s
        ON r.tx_id = s.tx_id
        JOIN (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                tx_succeeded,
                tx_id,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON r.tx_id = t.tx_id
    WHERE
        (
            COALESCE(
                c_old.amount,
                C.amount
            ) IS NOT NULL
            OR COALESCE(
                c_old.currency,
                C.currency
            ) IS NOT NULL
        )
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        r.tx_id,
        t.tx_succeeded,
        'SEI' AS transfer_type,
        r.msg_index,
        sender,
        amount,
        currency,
        receiver,
        _inserted_timestamp,
        concat_ws(
            '-',
            r.tx_id,
            r.msg_index,
            currency
        ) AS _unique_key
    FROM
        osmo_receiver r
        LEFT OUTER JOIN osmo_amount C
        ON r.tx_id = C.tx_id
        AND r.msg_index = C.msg_index
        LEFT OUTER JOIN sender s
        ON r.tx_id = s.tx_id
        JOIN (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                tx_succeeded,
                tx_id,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON r.tx_id = t.tx_id
    WHERE
        (
            amount IS NOT NULL
            OR currency IS NOT NULL
        )
    UNION ALL
    SELECT
        m.block_id,
        m.block_timestamp,
        s.tx_id,
        m.tx_succeeded,
        'IBC_TRANSFER_IN' AS transfer_type,
        m.msg_index,
        TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
        C.amount :: NUMBER AS amount,
        C.currency,
        TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
        m._inserted_timestamp,
        concat_ws(
            '-',
            s.tx_id,
            m.msg_index,
            currency
        ) AS _unique_key
    FROM
        sender s
        JOIN base_atts m
        ON s.tx_id = m.tx_id
        JOIN (
            SELECT
                DISTINCT block_id,
                block_timestamp,
                tx_succeeded,
                tx_id,
                _inserted_timestamp
            FROM
                base_atts
        ) t
        ON s.tx_id = t.tx_id
        INNER JOIN coin_sent_ibc C
        ON s.tx_id = C.tx_id
        AND m.msg_group = C.msg_group {# AND m.msg_sub_group = C.msg_sub_group #}
    WHERE
        TRY_PARSE_JSON(attribute_value) :sender :: STRING IS NOT NULL
        AND m.msg_type = 'write_acknowledgement'
        AND m.attribute_key = 'packet_data'
        AND (
            amount IS NOT NULL
            OR currency IS NOT NULL
        )
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.transfer_type,
    A.msg_index,
    A.sender,
    A.amount,
    A.currency,
    A.receiver,
    A._inserted_timestamp,
    A._unique_key
FROM
    fin A
