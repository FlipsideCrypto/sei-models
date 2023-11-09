{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
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
            'send_packet',
            'acknowledge_packet',
            'timeout_packet'
        )
        AND attribute_key IN (
            'packet_sequence',
            'packet_src_channel',
            'packet_dst_channel'
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
all_data AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :packet_sequence :: STRING AS packet_sequence,
        j :packet_src_channel :: STRING AS packet_src_channel,
        j :packet_dst_channel :: STRING AS packet_dst_channel
    FROM
        base_atts
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        _inserted_timestamp
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    msg_index,
    msg_type,
    packet_sequence,
    packet_src_channel,
    packet_dst_channel,
    _inserted_timestamp
FROM
    all_data
