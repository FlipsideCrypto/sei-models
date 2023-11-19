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
            'transfer',
            'ibc_transfer',
            'write_acknowledgement'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= DATEADD(
    DAY,
    -1,
    (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
)
{% endif %}
),
all_packets AS (
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        msg_type,
        packet_sequence,
        packet_src_channel,
        packet_dst_channel
    FROM
        {{ ref('silver__transfers_ibc_packets') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= DATEADD(
        DAY,
        -2,
        (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% endif %}
),
ack_pack AS (
    SELECT
        packet_sequence,
        packet_src_channel,
        packet_dst_channel
    FROM
        all_packets
    WHERE
        msg_type = 'acknowledge_packet' qualify(ROW_NUMBER() over(PARTITION BY packet_sequence, packet_src_channel, packet_dst_channel
    ORDER BY
        block_timestamp) = 1)
),
successful_sends AS (
    SELECT
        A.tx_id,
        A.msg_group
    FROM
        all_packets A
        JOIN ack_pack b
        ON A.packet_sequence = b.packet_sequence
        AND A.packet_src_channel = b.packet_src_channel
        AND A.packet_dst_channel = b.packet_dst_channel
    WHERE
        A.msg_type = 'send_packet'
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
ibc_out_transfers AS (
    SELECT
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index,
        OBJECT_AGG(
            A.attribute_key :: STRING,
            A.attribute_value :: variant
        ) AS j,
        j :sender :: STRING AS sender,
        j :receiver :: STRING AS receiver
    FROM
        base_atts A
        JOIN successful_sends b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
    WHERE
        A.msg_type = 'ibc_transfer'
    GROUP BY
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        A.msg_index
),
ibc_in_transfers AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            TRY_PARSE_JSON(
                attribute_value
            ) :: variant
        ) AS j,
        j :packet_data :amount :: INT amount,
        j :packet_data :denom :: STRING denom,
        j :packet_data :receiver :: STRING receiver,
        j :packet_data :sender :: STRING sender
    FROM
        base_atts
    WHERE
        msg_type = 'write_acknowledgement'
        AND attribute_key = 'packet_data'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group,
        msg_index
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
        RIGHT(A.amount, LENGTH(A.amount) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(A.amount, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        b_out.receiver AS ibc_out_receiver,
        c_in.sender AS ibc_in_sender
    FROM
        all_transfers A
        LEFT JOIN ibc_out_transfers b_out
        ON A.tx_id = b_out.tx_id
        AND A.msg_group = b_out.msg_group
        AND A.sender = b_out.sender
        LEFT JOIN ibc_in_transfers c_in
        ON A.tx_id = c_in.tx_id
        AND A.msg_group = c_in.msg_group
        AND A.msg_sub_group = c_in.msg_sub_group
        AND A.recipient = c_in.receiver
        JOIN sender s
        ON A.tx_id = s.tx_id
    WHERE
        (
            b_out.tx_id IS NOT NULL
            OR c_in.tx_id IS NOT NULL
        )
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    CASE
        WHEN ibc_out_receiver IS NOT NULL THEN 'IBC_TRANSFER_OUT'
        WHEN ibc_in_sender IS NOT NULL THEN 'IBC_TRANSFER_IN'
    END AS transfer_type,
    msg_index,
    COALESCE(
        ibc_in_sender,
        sender
    ) sender,
    COALESCE(
        ibc_out_receiver,
        receiver
    ) AS receiver,
    amount_int :: INT AS amount,
    currency,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS transfers_ibc_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_fin
WHERE
    amount IS NOT NULL
