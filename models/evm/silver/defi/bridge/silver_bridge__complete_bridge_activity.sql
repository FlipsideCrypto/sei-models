-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform','version'],
    cluster_by = ['block_timestamp::DATE','platform'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain_id, destination_chain, token_address, token_symbol), SUBSTRING(origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain, token_address, token_symbol)",
    tags = ['silver_bridge','defi','bridge','curated','heal','complete']
) }}

WITH contracts AS (

    SELECT
        address AS contract_address,
        symbol AS token_symbol,
        decimals AS token_decimals,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core_evm__dim_contracts') }}
),
prices AS (
    SELECT
        token_address,
        price,
        HOUR,
        is_verified,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('price__ez_prices_hourly') }}
),
ccip AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        platform,
        protocol,
        version,
        TYPE,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__ccip_send_requested') }}

{% if is_incremental() and 'ccip' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
cctp_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        platform,
        protocol,
        version,
        TYPE,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__cctp_v2_depositforburn') }}

{% if is_incremental() and 'cctp_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
layerzero_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        platform,
        protocol,
        version,
        TYPE,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__layerzero_v2') }}

{% if is_incremental() and 'layerzero_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
stargate_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        platform,
        protocol,
        version,
        TYPE,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2') }}

{% if is_incremental() and 'stargate_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
all_protocols AS (
    SELECT
        *
    FROM
        ccip
    UNION ALL
    SELECT
        *
    FROM
        cctp_v2
    UNION ALL
    SELECT
        *
    FROM
        layerzero_v2
    UNION ALL
    SELECT
        *
    FROM
        stargate_v2
),
all_bridges AS (
    SELECT
        *
    FROM
        all_protocols
),
complete_bridge_activity AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        protocol,
        version,
        TYPE,
        sender,
        receiver,
        destination_chain_receiver,
        CASE
            WHEN destination_chain_id :: STRING IS NULL THEN d.chain_id :: STRING
            ELSE destination_chain_id :: STRING
        END AS destination_chain_id,
        CASE
            WHEN destination_chain :: STRING IS NULL THEN LOWER(
                d.chain :: STRING
            )
            ELSE LOWER(
                destination_chain :: STRING
            )
        END AS destination_chain,
        b.token_address,
        CASE
            WHEN platform = 'axelar-v1' THEN COALESCE(
                C.token_symbol,
                b.token_symbol
            )
            ELSE C.token_symbol
        END AS token_symbol,
        C.token_decimals AS token_decimals,
        amount_unadj,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN (amount_unadj / pow(10, C.token_decimals))
            ELSE amount_unadj
        END AS amount,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN ROUND(
                amount * p.price,
                2
            )
            ELSE NULL
        END AS amount_usd,
        p.is_verified AS token_is_verified,
        _id,
        b._inserted_timestamp
    FROM
        all_bridges b
        LEFT JOIN contracts C
        ON b.token_address = C.contract_address
        LEFT JOIN prices p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ source(
            'external_gold_defillama',
            'dim_chains'
        ) }}
        d
        ON d.chain_id :: STRING = b.destination_chain_id :: STRING
        OR LOWER(
            d.chain
        ) = LOWER(
            b.destination_chain
        )
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        protocol,
        version,
        TYPE,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id,
        destination_chain,
        t0.token_address,
        C.token_symbol AS token_symbol,
        C.token_decimals AS token_decimals,
        amount_unadj,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN (amount_unadj / pow(10, C.token_decimals))
            ELSE amount_unadj
        END AS amount_heal,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN amount_heal * p.price
            ELSE NULL
        END AS amount_usd_heal,
        p.is_verified AS token_is_verified,
        _id,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN contracts C
        ON t0.token_address = C.contract_address
        LEFT JOIN prices p
        ON t0.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
    WHERE
        CONCAT(
            t0.block_number,
            '-',
            t0.platform,
            '-',
            t0.version
        ) IN (
            SELECT
                CONCAT(
                    t1.block_number,
                    '-',
                    t1.platform,
                    '-',
                    t1.version
                )
            FROM
                {{ this }}
                t1
            WHERE
                t1.token_decimals IS NULL
                AND t1._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        contracts C
                    WHERE
                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.token_decimals IS NOT NULL
                        AND C.contract_address = t1.token_address)
                    GROUP BY
                        1
                )
                OR CONCAT(
                    t0.block_number,
                    '-',
                    t0.platform,
                    '-',
                    t0.version
                ) IN (
                    SELECT
                        CONCAT(
                            t2.block_number,
                            '-',
                            t2.platform,
                            '-',
                            t2.version
                        )
                    FROM
                        {{ this }}
                        t2
                    WHERE
                        t2.amount_usd IS NULL
                        AND t2._inserted_timestamp < (
                            SELECT
                                MAX(
                                    _inserted_timestamp
                                ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                            FROM
                                {{ this }}
                        )
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                prices p
                            WHERE
                                p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                AND p.price IS NOT NULL
                                AND p.token_address = t2.token_address
                                AND p.hour = DATE_TRUNC(
                                    'hour',
                                    t2.block_timestamp
                                )
                        )
                    GROUP BY
                        1
                )
                OR CONCAT(
                    t0.block_number,
                    '-',
                    t0.platform,
                    '-',
                    t0.version
                ) IN (
                    SELECT
                        CONCAT(
                            t3.block_number,
                            '-',
                            t3.platform,
                            '-',
                            t3.version
                        )
                    FROM
                        {{ this }}
                        t3
                    WHERE
                        t3.token_address IN (
                            SELECT
                                token_address
                            FROM
                                {{ ref('price__ez_asset_metadata') }}
                            WHERE
                                IFNULL(
                                    is_verified_modified_timestamp,
                                    '1970-01-01' :: TIMESTAMP
                                ) > DATEADD('day', -10, SYSDATE()))
                        )
                ),
            {% endif %}

            FINAL AS (
                SELECT
                    *
                FROM
                    complete_bridge_activity

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    protocol,
    version,
    TYPE,
    sender,
    receiver,
    destination_chain_receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    token_decimals,
    amount_unadj,
    amount_heal AS amount,
    amount_usd_heal AS amount_usd,
    token_is_verified,
    _id,
    _inserted_timestamp
FROM
    heal_model
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
    bridge_address,
    event_name,
    platform,
    protocol,
    version,
    TYPE,
    sender,
    receiver,
    destination_chain_receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    token_decimals,
    amount_unadj,
    amount,
    amount_usd,
    IFNULL(
        token_is_verified,
        FALSE
    ) AS token_is_verified,
    _id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['_id']
    ) }} AS complete_bridge_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    destination_chain <> 'sei' qualify (ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    _inserted_timestamp DESC)) = 1
