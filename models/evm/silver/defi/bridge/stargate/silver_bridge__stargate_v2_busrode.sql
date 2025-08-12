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
logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
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
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        (
            contract_address IN (
                SELECT
                    contract_address
                FROM
                    contract_mapping
            )
            AND topic_0 = '0x15955c5a4cc61b8fbb05301bce47fd31c0e6f935e1ab97fdac9b134c887bb074' --busRode
        )
        OR topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
oft_sent AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index AS oft_sent_index,
        LEAD(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS next_oft_sent_index,
        contract_address AS stargate_adapter_address,
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
        ) :: INT AS dst_id,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS amount_sent,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        modified_timestamp
    FROM
        logs
    WHERE
        topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'
        AND guid = '0000000000000000000000000000000000000000000000000000000000000000'
),
bus_raw AS (
    SELECT
        tx_hash,
        event_index AS bus_rode_index,
        LEAD(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS next_bus_rode_index,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS bus_dst_id,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS ticket_id,
        regexp_substr_all(SUBSTR(DATA, 195), '.{64}') AS passenger_raw,
        utils.udf_hex_to_int(
            passenger_raw [1] :: STRING
        ) * 2 AS passenger_length,
        SUBSTR(
            DATA,
            323,
            passenger_length
        ) AS passenger_final,
        utils.udf_hex_to_int(
            SUBSTR(
                passenger_final,
                1,
                4
            )
        ) AS asset_id,
        SUBSTR(
            passenger_final,
            5,
            64
        ) AS dst_receiver_address_raw,
        '0x' || SUBSTR(SUBSTR(passenger_final, 5, 64), 25) AS dst_receiver_address,
        SUBSTR(
            passenger_final,
            85,
            2
        ) AS is_native_drop
    FROM
        logs
    WHERE
        topic_0 = '0x15955c5a4cc61b8fbb05301bce47fd31c0e6f935e1ab97fdac9b134c887bb074'
)
SELECT
    o.block_number,
    o.block_timestamp,
    o.tx_hash,
    o.oft_sent_index,
    b.bus_rode_index,
    stargate_adapter_address,
    guid,
    from_address,
    dst_id,
    amount_sent,
    bus_dst_id,
    ticket_id,
    asset_id,
    A.asset AS asset_name,
    A.address AS asset_address,
    dst_receiver_address,
    is_native_drop,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    modified_timestamp
FROM
    oft_sent o
    INNER JOIN bus_raw b
    ON o.tx_hash = b.tx_hash
    AND o.oft_sent_index > b.bus_rode_index
    AND (
        o.oft_sent_index < b.next_bus_rode_index
        OR b.next_bus_rode_index IS NULL
    )
    LEFT JOIN {{ ref('silver_bridge__stargate_v2_asset_seed') }} A
    ON asset_id = id
    AND A.chain = 'sei'
