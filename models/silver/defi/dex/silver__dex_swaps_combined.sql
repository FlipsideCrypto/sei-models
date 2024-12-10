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
        'astroport' AS platform,
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        swapper,
        msg_index,
        amount_in,
        currency_in,
        amount_out,
        currency_out,
        pool_address,
        dex_swaps_astroport_id AS uk
    FROM
        {{ ref('silver__dex_swaps_astroport') }}

{% if is_incremental() and 'astroport' not in var('HEAL_MODELS') %}
WHERE
    modified_timestamp >= '{{ max_mod_timestamp }}'
{% endif %}
UNION ALL
SELECT
    'fuzio' AS platform,
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    swapper,
    msg_index,
    amount_in,
    currency_in,
    amount_out,
    currency_out,
    pool_address,
    dex_swaps_fuzio_id AS uk
FROM
    {{ ref('silver__dex_swaps_fuzio') }}

{% if is_incremental() and 'fuzio' not in var('HEAL_MODELS') %}
WHERE
    modified_timestamp >= '{{ max_mod_timestamp }}'
{% endif %}
UNION ALL
SELECT
    'seaswap' AS platform,
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    swapper,
    msg_index,
    amount_in,
    currency_in,
    amount_out,
    currency_out,
    pool_address,
    dex_swaps_seaswap_id AS uk
FROM
    {{ source(
        'silver',
        'dex_swaps_seaswap'
    ) }}

{% if is_incremental() and 'seaswap' not in var('HEAL_MODELS') %}
WHERE
    modified_timestamp >= '{{ max_mod_timestamp }}'
{% endif %}
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
        currency,
        decimals,
        symbol
    FROM
        {{ ref('core__dim_tokens') }}
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
        A.block_id,
        A.block_timestamp,
        A.tx_id AS tx_id,
        'sei' AS originated_from,
        A.platform,
        A.swapper,
        d.evm_address AS origin_from_address,
        A.pool_address,
        A.amount_in AS amount_in_unadj,
        CASE
            WHEN c_in.decimals IS NOT NULL THEN (amount_in_unadj / pow(10, c_in.decimals))
        END AS amount_in,
        CASE
            WHEN c_in.decimals IS NOT NULL THEN (amount_in_unadj / pow(10, c_in.decimals)) * b_in.price
        END AS amount_in_usd,
        A.amount_out AS amount_out_unadj,
        CASE
            WHEN c_out.decimals IS NOT NULL THEN (amount_out_unadj / pow(10, c_out.decimals))
        END AS amount_out,
        CASE
            WHEN c_out.decimals IS NOT NULL THEN (amount_out_unadj / pow(10, c_out.decimals)) * b_out.price
        END AS amount_out_usd,
        A.currency_in AS token_in,
        c_in.symbol AS symbol_in,
        A.currency_out AS token_out,
        c_out.symbol AS symbol_out,
        NULL AS origin_function_signature,
        A.msg_index AS INDEX,
        NULL AS origin_to_address,
        NULL AS sender,
        NULL AS tx_to,
        NULL AS event_name,
        uk AS swaps_combined_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        inc A
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        b_in
        ON A.currency_in = b_in.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_in.hour
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        b_out
        ON A.currency_out = b_out.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_out.hour
        LEFT JOIN {{ ref('core__dim_tokens') }}
        c_in
        ON A.currency_in = c_in.currency
        LEFT JOIN {{ ref('core__dim_tokens') }}
        c_out
        ON A.currency_out = c_out.currency
        LEFT JOIN {{ ref('core__dim_address_mapping') }}
        d
        ON A.swapper = d.sei_address

{% if is_incremental() %}
UNION ALL
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.originated_from,
    A.platform,
    A.swapper,
    COALESCE(
        A.origin_from_address,
        d.evm_address
    ) AS origin_from_address,
    A.pool_address,
    A.amount_in_unadj,
    COALESCE(
        A.amount_in,
        CASE
            WHEN c_in.decimals IS NOT NULL THEN (A.amount_in_unadj / pow(10, c_in.decimals))
        END
    ) AS amount_in,
    COALESCE(
        A.amount_in_usd,
        CASE
            WHEN c_in.decimals IS NOT NULL THEN (A.amount_in_unadj / pow(10, c_in.decimals)) * b_in.price
        END
    ) AS amount_in_usd,
    A.amount_out_unadj,
    COALESCE(
        A.amount_out,
        CASE
            WHEN c_out.decimals IS NOT NULL THEN (A.amount_out_unadj / pow(10, c_in.decimals))
        END
    ) AS amount_out,
    COALESCE(
        A.amount_out_usd,
        CASE
            WHEN c_out.decimals IS NOT NULL THEN (A.amount_out_unadj / pow(10, c_in.decimals)) * b_out.price
        END
    ) AS amount_out_usd,
    A.token_in,
    COALESCE(
        A.symbol_in,
        c_in.symbol
    ) AS symbol_in,
    A.token_out,
    COALESCE(
        A.symbol_out,
        c_out.symbol
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
    ON A.token_in = c_in.currency
    LEFT JOIN mod_decimal c_out
    ON A.token_out = c_out.currency
    LEFT JOIN mod_map d
    ON A.swapper = d.sei_address
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
        OR c_in.decimals IS NOT NULL
        OR c_out.decimals IS NOT NULL
        OR d.evm_address IS NOT NULL
    )
{% endif %}
)
SELECT
    *
FROM
    fin qualify(ROW_NUMBER() over (PARTITION BY swaps_combined_id
ORDER BY
    inserted_timestamp DESC) = 1)
