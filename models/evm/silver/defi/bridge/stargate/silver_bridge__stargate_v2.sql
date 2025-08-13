-- depends_on: {{ ref('silver_bridge__stargate_v2_asset_seed') }}
-- depends_on: {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH bus_driven AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        event_name,
        stargate_adapter_address AS bridge_address,
        guid,
        ticket_id,
        from_address AS sender,
        dst_receiver_address AS receiver,
        dst_receiver_address AS destination_chain_receiver,
        dst_chain_id,
        dst_chain_id :: STRING AS destination_chain_id,
        dst_chain AS destination_chain,
        asset_address AS token_address,
        NULL AS token_symbol,
        amount_sent AS amount_unadj,
        src_chain_id,
        src_chain,
        asset_id,
        asset_name,
        payload,
        tx_type,
        nonce,
        sender_contract_address,
        receiver_contract_address,
        message_type,
        protocol,
        version,
        TYPE,
        platform,
        CONCAT(
            tx_hash,
            '-',
            event_index,
            '-',
            ticket_id
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2_busdriven') }}

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
oft_sent AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        'OFTSent' AS event_name,
        contract_address AS bridge_address,
        guid,
        NULL AS ticket_id,
        from_address AS sender,
        to_address AS receiver,
        to_address AS destination_chain_receiver,
        dst_chain_id,
        dst_chain_id :: STRING AS destination_chain_id,
        dst_chain AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_sent AS amount_unadj,
        src_chain_id,
        src_chain,
        asset_id,
        asset_name,
        payload,
        tx_type,
        nonce,
        sender_contract_address,
        receiver_contract_address,
        message_type,
        protocol,
        version,
        TYPE,
        platform,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2_oft') }}

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
combined AS (
    SELECT
        *
    FROM
        bus_driven
    UNION ALL
    SELECT
        *
    FROM
        oft_sent
),

{% if is_incremental() and 'stargate_heal' in var('HEAL_MODELS') %}
blocks_to_heal AS (
    SELECT
        DISTINCT block_number
    FROM
        {{ this }}
    WHERE
        token_address IS NULL
        OR destination_chain IS NULL
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        combined

{% if is_incremental() and 'stargate_heal' in var('HEAL_MODELS') %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    bridge_address,
    guid,
    ticket_id,
    sender,
    receiver,
    destination_chain_receiver,
    dst_chain_id,
    destination_chain_id,
    LOWER(
        b.chain
    ) AS destination_chain,
    A.address AS token_address,
    A.asset AS token_symbol,
    amount_unadj,
    src_chain_id,
    src_chain,
    A.id AS asset_id,
    A.asset AS asset_name,
    payload,
    tx_type,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    protocol,
    version,
    TYPE,
    platform,
    _log_id,
    modified_timestamp
FROM
    {{ this }}
    t
    INNER JOIN blocks_to_heal USING (block_number)
    LEFT JOIN {{ ref('silver_bridge__stargate_v2_asset_seed') }} A
    ON t.bridge_address = A.oftaddress
    AND A.chain = 'sei'
    LEFT JOIN {{ ref('silver_bridge__layerzero_v2_bridge_seed') }}
    b
    ON t.destination_chain_id = b.eid
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
    bridge_address,
    guid,
    ticket_id,
    sender,
    receiver,
    destination_chain_receiver,
    dst_chain_id,
    destination_chain_id,
    destination_chain,
    A.address AS token_address,
    A.asset AS token_symbol,
    amount_unadj,
    src_chain_id,
    src_chain,
    asset_id,
    asset_name,
    payload,
    tx_type,
    nonce,
    sender_contract_address,
    receiver_contract_address,
    message_type,
    protocol,
    version,
    TYPE,
    platform,
    _log_id,
    modified_timestamp
FROM
    FINAL f
    LEFT JOIN {{ ref('silver_bridge__stargate_v2_asset_seed') }} A
    ON f.bridge_address = A.oftaddress
    AND A.chain = 'sei' qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            modified_timestamp DESC,
            destination_chain DESC nulls last,
            token_address DESC nulls last
    ) = 1
