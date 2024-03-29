{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id","msg_group","msg_sub_group"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    enabled = false
) }}

WITH msg_atts AS (

    SELECT
        block_timestamp,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_type,
        msg_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'wasm'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
prefinal AS (
    SELECT
        A.block_timestamp,
        A.tx_id,
        msg_group,
        msg_sub_group,
        {# A.msg_index, #}
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :amount :: INT AS amount,
        j :from :: STRING AS from_address,
        j :"transfer.amount" :: INT AS transfer_amount,
        j :"transfer.block_time" :: INT AS transfer_block_time,
        j :"transfer.nonce" :: INT AS transfer_nonce,
        j :"transfer.recipient" :: STRING AS transfer_recipient,
        j :"transfer.recipient_chain" :: INT AS transfer_recipient_chain,
        j :"transfer.sender" :: STRING AS transfer_sender,
        j :"transfer.token" :: STRING AS transfer_token,
        j :"transfer.token_chain" :: INT AS transfer_token_chain,
        j :"message.message" :: STRING AS message_message,
        j :"message.nonce" :: INT AS message_nonce,
        j :"message.sender" :: STRING AS message_sender,
        j :"message.sequence" :: INT AS message_sequence,
        _inserted_timestamp
    FROM
        msg_atts A
        JOIN (
            SELECT
                tx_id,
                block_timestamp :: DATE bd
            FROM
                msg_atts
            WHERE
                msg_type = 'wasm'
                AND attribute_key = '_contract_address'
                AND attribute_value = 'sei1smzlm9t79kur392nu9egl8p8je9j92q4gzguewj56a05kyxxra0qy0nuf3'
        ) b
        ON A.block_timestamp :: DATE = b.bd
        AND A.tx_id = b.tx_id
        JOIN (
            SELECT
                tx_id,
                block_timestamp :: DATE bd
            FROM
                msg_atts
            WHERE
                msg_type = 'wasm'
                AND attribute_key = 'action'
                AND attribute_value = 'burn_from'
        ) C
        ON A.block_timestamp :: DATE = C.bd
        AND A.tx_id = C.tx_id
    WHERE
        attribute_key NOT IN (
            '_contract_address',
            'action',
            'amount'
        )
    GROUP BY
        A.block_timestamp,
        A.tx_id,
        msg_group,
        msg_sub_group,
        {# A.msg_index, #}
        _inserted_timestamp
),
con_array AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        ARRAY_AGG(
            DISTINCT attribute_value
        ) within GROUP(
            ORDER BY
                attribute_value
        ) AS _contract_address
    FROM
        msg_atts A
    WHERE
        attribute_key = '_contract_address'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group
),
act_array AS (
    SELECT
        tx_id,
        msg_group,
        msg_sub_group,
        ARRAY_AGG(
            DISTINCT attribute_value
        ) within GROUP(
            ORDER BY
                attribute_value
        ) AS action
    FROM
        msg_atts A
    WHERE
        attribute_key = 'action'
    GROUP BY
        tx_id,
        msg_group,
        msg_sub_group
),
amount AS (
    SELECT
        DISTINCT tx_id,
        msg_group,
        msg_sub_group,
        attribute_value amount
    FROM
        msg_atts A
    WHERE
        attribute_key = 'amount'
)
SELECT
    A.block_timestamp,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    b._contract_address AS contract_address_array,
    C.action AS action_array,
    d.amount,
    from_address,
    transfer_amount,
    transfer_block_time,
    transfer_nonce,
    transfer_recipient,
    transfer_recipient_chain,
    transfer_sender,
    transfer_token,
    transfer_token_chain,
    message_message,
    message_nonce,
    message_sender,
    message_sequence,
    _inserted_timestamp
FROM
    prefinal A
    JOIN con_array b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
    JOIN act_array C
    ON A.tx_id = C.tx_id
    AND A.msg_group = C.msg_group
    AND A.msg_sub_group = C.msg_sub_group
    JOIN amount d
    ON A.tx_id = d.tx_id
    AND A.msg_group = d.msg_group
    AND A.msg_sub_group = d.msg_sub_group
