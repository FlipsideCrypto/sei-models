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
bus_driven_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        'BusDriven' AS event_name,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS dst_id,
        part [3] :: STRING AS guid,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS start_ticket_id,
        utils.udf_hex_to_int(
            part [2] :: STRING
        ) :: INT AS num_passengers,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0x1623f9ea59bd6f214c9571a892da012fc23534aa5906bef4ae8c5d15ee7d2d6e' --event_name = 'BusDriven'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
bus_driven_array AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        dst_id,
        guid,
        num_passengers,
        start_ticket_id,
        VALUE :: INT AS ticket_id,
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
        bus_driven_raw,
        LATERAL FLATTEN (
            input =>(
                array_generate_range(
                    start_ticket_id,
                    start_ticket_id + num_passengers,
                    1
                )
            )
        )
),
bus_driven AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.tx_hash,
        r.event_index,
        r.event_name,
        r.guid,
        contract_address,
        stargate_adapter_address,
        r.dst_id,
        start_ticket_id,
        num_passengers,
        b.ticket_id,
        asset_id,
        asset_name,
        asset_address,
        from_address,
        dst_receiver_address,
        b.amount_sent,
        r.origin_from_address,
        r.origin_to_address,
        r.origin_function_signature,
        r.protocol,
        r.version,
        r.type,
        r.platform,
        r._log_id,
        r.modified_timestamp
    FROM
        bus_driven_array r
        INNER JOIN {{ ref('silver_bridge__stargate_v2_busrode') }}
        b USING (
            dst_id,
            ticket_id
        )

{% if is_incremental() %}
WHERE
    b.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
layerzero AS (
    SELECT
        tx_hash,
        guid,
        payload,
        TYPE AS tx_type,
        nonce,
        src_chain_id,
        src_chain,
        sender_contract_address,
        dst_chain_id,
        dst_chain,
        receiver_contract_address,
        message_type
    FROM
        {{ ref('silver_bridge__layerzero_v2_packet') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    guid,
    b.event_index,
    b.event_name,
    b.contract_address,
    stargate_adapter_address,
    dst_id,
    start_ticket_id,
    num_passengers,
    ticket_id,
    asset_id,
    asset_name,
    asset_address,
    from_address,
    dst_receiver_address,
    amount_sent,
    payload,
    tx_type,
    nonce,
    src_chain_id,
    src_chain,
    sender_contract_address,
    dst_chain_id,
    dst_chain,
    receiver_contract_address,
    message_type,
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
    bus_driven b
    INNER JOIN layerzero l USING (
        tx_hash,
        guid
    )
