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
            '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d',
            'circle_cctp',
            'v2',
            'deposit'
        ) AS t(
            contract_address,
            protocol,
            version,
            TYPE
        )
),
base_evt AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_index,
        topic_0,
        'DepositForBurn' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        '0x' || SUBSTR(
            topic_1,
            27
        ) AS burnToken,
        '0x' || SUBSTR(
            topic_2,
            27
        ) AS depositor,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            part [2] :: STRING
        ) :: INT AS destination_domain,
        CASE
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(
                part [1] :: STRING
            ) -- solana
            WHEN LEFT(
                part [1] :: STRING,
                24
            ) = '000000000000000000000000' THEN CONCAT(
                '0x',
                SUBSTR(
                    part [1] :: STRING,
                    25,
                    40
                )
            ) -- evm
            ELSE part [1] :: STRING -- other non-evm chains
        END AS mint_recipient,
        CASE
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(
                part [3] :: STRING
            ) -- solana
            WHEN LEFT(
                part [3] :: STRING,
                24
            ) = '000000000000000000000000' THEN CONCAT(
                '0x',
                SUBSTR(
                    part [3] :: STRING,
                    25,
                    40
                )
            ) -- evm
            ELSE part [3] :: STRING -- other non-evm chains
        END AS destination_token_messenger,
        CASE
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(
                part [4] :: STRING
            ) -- solana
            WHEN LEFT(
                part [4] :: STRING,
                24
            ) = '000000000000000000000000' THEN CONCAT(
                '0x',
                SUBSTR(
                    part [4] :: STRING,
                    25,
                    40
                )
            ) -- evm
            ELSE part [4] :: STRING -- other non-evm chains
        END AS destination_caller,
        protocol,
        version,
        TYPE,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        modified_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        INNER JOIN contract_mapping USING (contract_address)
    WHERE
        topic_0 = '0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 days'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    contract_address AS bridge_address,
    burnToken AS token_address,
    amount AS amount_unadj,
    depositor AS sender,
    origin_from_address AS receiver,
    mint_recipient AS destination_chain_receiver,
    destination_domain AS destination_chain_id,
    chain AS destination_chain,
    protocol,
    version,
    TYPE,
    platform,
    _log_id,
    modified_timestamp
FROM
    base_evt
    LEFT JOIN {{ ref('silver_bridge__cctp_chain_id_seed') }}
    d
    ON domain = destination_domain
