{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH contract_mapping AS (

    SELECT
        *
    FROM
    VALUES
        (
            '0xaba60da7e88f7e8f5868c2b6de06cb759d693af0',
            'chainlink_ccip',
            'v1',
            'router'
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
        'OnRampSet' AS event_name,
        l.contract_address AS bridge_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        topic_1,
        '0x' || SUBSTR(
            part [0] :: STRING,
            25
        ) AS on_ramp_address,
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
        {{ ref('core_evm__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topic_0 = '0x1f7d0ec248b80e5c0dde0ee531c4fc8fdb6ce9a2b3d90f560c74acd6a7202f23' -- onrampset
        AND tx_succeeded
        AND event_removed = FALSE
        AND block_timestamp :: DATE >= '2023-01-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 days'
{% endif %}
),
FINAL AS (
    SELECT
        *,
        utils.udf_hex_to_int(SUBSTR(topic_1 :: STRING, 3)) :: STRING AS dest_chain_selector,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS rn
    FROM
        logs
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_name,
    bridge_address,
    dest_chain_selector,
    on_ramp_address,
    chain_name,
    protocol,
    version,
    TYPE,
    platform,
    modified_timestamp
FROM
    FINAL
    INNER JOIN {{ ref('silver_bridge__ccip_chain_seed') }}
    ON dest_chain_selector :: STRING = chain_selector :: STRING
