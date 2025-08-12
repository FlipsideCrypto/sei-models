{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['noncore']
) }}

WITH contract_mapping AS (

    SELECT
        *
    FROM
    VALUES
        (
            '0x1502fa4be69d526124d453619276faccab275d3d',
            'stargate',
            'v2',
            'bridge'
        ) AS t(
            contract_address,
            protocol,
            version,
            TYPE
        )
),
layerzero AS (
    SELECT
        tx_hash,
        payload,
        tx_type,
        nonce,
        src_chain_id,
        src_chain,
        sender_contract_address,
        dst_chain_id,
        dst_chain,
        receiver_contract_address,
        guid,
        message_type,
        '0x' || SUBSTR(SUBSTR(payload, 227, 64), 25) AS to_address,
        protocol,
        version,
        TYPE,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        _log_id,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}
    WHERE
        sender_contract_address NOT IN (
            SELECT
                contract_address
            FROM
                contract_mapping
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
oft_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        contract_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        SUBSTR(
            topic_1,
            3
        ) AS guid,
        '0x' || SUBSTR(
            topic_2,
            27
        ) AS from_address,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS dst_chain_id_oft,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS amount_sent,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        'OFTSent' AS event_name,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    contract_address AS bridge_address,
    guid,
    from_address AS sender,
    to_address AS receiver,
    to_address AS destination_chain_receiver,
    dst_chain_id,
    dst_chain_id :: STRING AS destination_chain_id,
    dst_chain AS destination_chain,
    COALESCE(
        token_address,
        contract_address
    ) AS token_address,
    amount_sent AS amount_unadj,
    src_chain_id,
    src_chain,
    payload,
    tx_type,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    l.protocol,
    l.version,
    l.type,
    l.platform,
    o._log_id,
    o.modified_timestamp
FROM
    oft_raw o
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
    INNER JOIN {{ ref('silver_bridge__layerzero_v2_token_reads') }} USING (
        contract_address
    )
