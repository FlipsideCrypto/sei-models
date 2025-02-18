{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'swaps_combined_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
-- get the max modified_timestamp from the target table
{% set max_m_query %}

SELECT
    MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "30 minutes") }}' AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod_timestamp = run_query(max_m_query).columns [0].values() [0] %}
{% endif %}
{% endif %}

WITH inc AS (
    SELECT
        'dragonswap' AS platform,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        tx_to,
        sender,
        amount_in_unadj,
        amount_out_unadj,
        token_in,
        token_out,
        dragonswap_swaps_decoded_id AS uk
    FROM
        {{ ref('silver_evm_dex__dragonswap_swaps_combined') }}

{% if is_incremental() and 'dragonswap' not in var('HEAL_MODELS') %}
WHERE
    modified_timestamp >= '{{ max_mod_timestamp }}'
{% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        event_index
        ORDER BY
            modified_timestamp DESC
    ) = 1 
    -- add other dexes

    UNION ALL

    SELECT
        'jellyswap' AS platform,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        pool_address AS contract_address,
        null AS tx_to,
        null AS sender,
        amount_in_unadj,
        amount_out_unadj,
        token_in,
        token_out,
        jellyswap_swaps_id AS uk

    FROM
        {{ ref('silver_evm_dex__jellyswap_swaps') }}

    UNION ALL

     SELECT
        'sailorswap' AS platform,
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        pool_address AS contract_address,
        recipient AS tx_to,
        sender,
        ABS(amount0) AS amount_in_unadj,
        ABS(amount1) AS amount_out_unadj,
        token0 AS token_in,
        token1 AS token_out,
        sailorswap_swaps_id AS uk
    FROM
        {{ ref('silver_evm_dex__sailorswap_swaps') }}
)

{% if is_incremental() %},
mod_price AS (
    SELECT
        token_address,
        HOUR,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        modified_timestamp >= '{{ max_mod_timestamp }}'
),
mod_decimal AS (
    SELECT
        contract_address,
        token_decimals,
        token_symbol
    FROM
        {{ ref('silver_evm__contracts') }}
    WHERE
        modified_timestamp >= '{{ max_mod_timestamp }}'
),
mod_map AS (
    SELECT
        evm_address,
        sei_address
    FROM
        {{ ref('core__dim_address_mapping') }}
    WHERE
        modified_timestamp >= '{{ max_mod_timestamp }}'
)
{% endif %},
fin AS (
    SELECT
        A.block_number AS block_id,
        A.block_timestamp,
        A.tx_hash AS tx_id,
        'evm' AS originated_from,
        A.platform,
        d.sei_address AS swapper,
        A.origin_from_address,
        A.contract_address AS pool_address,
        A.amount_in_unadj,
        CASE
            WHEN c_in.token_decimals IS NOT NULL THEN (amount_in_unadj / pow(10, c_in.token_decimals))
        END AS amount_in,
        CASE
            WHEN c_in.token_decimals IS NOT NULL THEN amount_in * b_in.price
        END AS amount_in_usd,
        A.amount_out_unadj,
        CASE
            WHEN c_out.token_decimals IS NOT NULL THEN (amount_out_unadj / pow(10, c_out.token_decimals))
        END AS amount_out,
        CASE
            WHEN c_out.token_decimals IS NOT NULL THEN amount_out * b_out.price
        END AS amount_out_usd,
        A.token_in,
        c_in.token_symbol AS symbol_in,
        A.token_out,
        c_out.token_symbol AS symbol_out,
        A.origin_function_signature,
        A.event_index AS INDEX,
        A.origin_to_address,
        A.sender,
        A.tx_to,
        A.event_name,
        uk AS swaps_combined_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        inc A
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        b_in
        ON A.token_in = b_in.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_in.hour
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        b_out
        ON A.token_out = b_out.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_out.hour
        LEFT JOIN {{ ref('silver_evm__contracts') }}
        c_in
        ON A.token_in = c_in.contract_address
        LEFT JOIN {{ ref('silver_evm__contracts') }}
        c_out
        ON A.token_out = c_out.contract_address
        LEFT JOIN {{ ref('core__dim_address_mapping') }}
        d
        ON A.origin_from_address = d.evm_address

{% if is_incremental() %}
UNION ALL
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.originated_from,
    A.platform,
    COALESCE(
        A.swapper,
        d.sei_address
    ) AS swapper,
    A.origin_from_address,
    A.pool_address,
    A.amount_in_unadj,
    COALESCE(
        A.amount_in,
        CASE
            WHEN c_in.token_decimals IS NOT NULL THEN (A.amount_in_unadj / pow(10, c_in.token_decimals))
        END
    ) AS amount_in,
    COALESCE(
        A.amount_in_usd,
        CASE
            WHEN c_in.token_decimals IS NOT NULL THEN (A.amount_in_unadj / pow(10, c_in.token_decimals)) * b_in.price
        END
    ) AS amount_in_usd,
    A.amount_out_unadj,
    COALESCE(
        A.amount_out,
        CASE
            WHEN c_out.token_decimals IS NOT NULL THEN (A.amount_out_unadj / pow(10, c_in.token_decimals))
        END
    ) AS amount_out,
    COALESCE(
        A.amount_out_usd,
        CASE
            WHEN c_out.token_decimals IS NOT NULL THEN (A.amount_out_unadj / pow(10, c_in.token_decimals)) * b_out.price
        END
    ) AS amount_out_usd,
    A.token_in,
    COALESCE(
        A.symbol_in,
        c_in.token_symbol
    ) AS symbol_in,
    A.token_out,
    COALESCE(
        A.symbol_out,
        c_out.token_symbol
    ) AS symbol_out,
    A.origin_function_signature,
    A.index,
    A.origin_to_address,
    A.sender,
    A.tx_to,
    A.event_name,
    A.swaps_combined_id,
    A.inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ this }} A
    LEFT JOIN mod_price b_in
    ON A.token_in = b_in.token_address
    AND DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = b_in.hour
    LEFT JOIN mod_price b_out
    ON A.token_out = b_out.token_address
    AND DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = b_out.hour
    LEFT JOIN mod_decimal c_in
    ON A.token_in = c_in.contract_address
    LEFT JOIN mod_decimal c_out
    ON A.token_out = c_out.contract_address
    LEFT JOIN mod_map d
    ON A.origin_from_address = d.evm_address
WHERE
    (
        A.amount_in IS NULL
        OR A.amount_in_usd IS NULL
        OR A.amount_out IS NULL
        OR A.amount_out_usd IS NULL
        OR A.symbol_in IS NULL
        OR A.symbol_out IS NULL
        OR A.swapper IS NULL
    )
    AND (
        b_in.price IS NOT NULL
        OR b_out.price IS NOT NULL
        OR c_in.token_decimals IS NOT NULL
        OR c_out.token_decimals IS NOT NULL
        OR d.sei_address IS NOT NULL
    )
{% endif %}
)
SELECT
    *
FROM
    fin qualify(ROW_NUMBER() over (PARTITION BY swaps_combined_id
ORDER BY
    inserted_timestamp DESC) = 1)
