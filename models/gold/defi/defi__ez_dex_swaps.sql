{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['noncore','recent_test']
) }}

{% if execute %}

{% if is_incremental() %}
-- get the max modified_timestamp from the target table
{% set max_m_query %}

SELECT
    MAX(modified_timestamp) - INTERVAL '15 minutes' AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod_timestamp = run_query(max_m_query).columns [0].values() [0] %}
{% endif %}

{# --create temp table for dragoswap
{% set query = """ CREATE OR REPLACE TEMPORARY TABLE gold.ez_dex_swaps__dragonswap_intermediate_tmp AS SELECT * FROM """ ~ ref('silver_evm_dex__dragonswap_swaps_combined') %}
{% set incr = "" %}

{% if is_incremental() and 'dragonswap' not in var('HEAL_MODELS') %}
{% set incr = """ WHERE modified_timestamp >= '""" ~ max_mod_timestamp ~ """' """ %}
{% endif %}

{% do run_query(
    query ~ incr
) %}
#}
{% endif %}

WITH inc_evm AS (
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
        token_out
    FROM
        {{ ref('silver_evm_dex__dragonswap_swaps_combined') }}
        qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                modified_timestamp DESC
        ) = 1 -- add other dexes
) {#

{% if is_incremental() %},
price_updates AS (
    SELECT
        A.platform,
        A.block_number,
        A.block_timestamp,
        A.tx_hash,
        A.event_index,
        A.origin_function_signature,
        A.origin_from_address,
        A.origin_to_address,
        A.contract_address,
        A.tx_to,
        A.sender,
        A.amount_in_unadj,
        A.amount_out_unadj,
        A.token_in,
        A.token_out
    FROM
        {{ this }} A
        LEFT JOIN mod_prices b_in
        ON A.token_in = b_in.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_in.hour
        LEFT JOIN mod_prices b_out
        ON A.token_out = b_out.token_address
        AND DATE_TRUNC(
            'hour',
            A.block_timestamp
        ) = b_out.hour
    WHERE
        (
            b_in.token_address IS NOT NULL
            OR b_out.token_address IS NOT NULL
        )
        AND GREATEST(COALESCE(b_in.modified_timestamp, '2000-01-01'), COALESCE(b_in.modified_timestamp, '2000-01-01')) >= '{{max_mod_timestamp}}'
)
{% endif %}

#},
final_evm AS (
    SELECT
        A.block_number AS block_id,
        A.block_timestamp,
        NULL AS tx_id,
        A.tx_hash,
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
        A.event_name
    FROM
        inc_evm A
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
)
SELECT
    *
FROM
    final_evm
