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
        SUBSTR(
            payload,
            229,
            4
        ) AS message_type_2,
        '0x' || SUBSTR(SUBSTR(payload, 233, 64), 25) AS to_address,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}
        l
        INNER JOIN contract_mapping m
        ON l.sender_contract_address = m.contract_address

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
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
        contract_address AS stargate_oft_address,
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
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
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
    tx_hash,
    guid,
    event_index,
    'OFTSent' AS event_name,
    stargate_oft_address,
    stargate_oft_address AS contract_address,
    A.address AS token_address,
    A.id AS asset_id,
    A.asset AS asset_name,
    from_address,
    to_address,
    src_chain_id,
    src_chain,
    dst_chain_id,
    dst_chain,
    dst_chain_id_oft,
    amount_sent,
    payload,
    tx_type,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    message_type_2,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    protocol,
    version,
    TYPE,
    platform,
    _log_id,
    modified_timestamp
FROM
    oft_raw o
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
    LEFT JOIN {{ ref('silver_bridge__stargate_v2_asset_seed') }} A
    ON o.stargate_oft_address = A.oftaddress
    AND A.chain = 'sei'
