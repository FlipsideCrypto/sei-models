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

--create temp table for dragoswap
{% set query = """ CREATE OR REPLACE TEMPORARY TABLE gold.ez_dex_swaps__dragonswap_intermediate_tmp AS SELECT * FROM """ ~ ref('silver_evm_dex__dragonswap_swaps_combined') %}
{% set incr = "" %}

{% if is_incremental() and 'dragonswap' not in var('HEAL_MODELS') %}
{% set incr = """ WHERE modified_timestamp >= '""" ~ max_mod_timestamp ~ """' """ %}
{% endif %}

{% do run_query(
    query ~ incr
) %}
{% endif %}
SELECT
    'dragonswap' AS platform block_number,
    block_timestamp,
    tx_hash,
    event_index,
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
    gold.ez_dex_swaps__dragonswap_intermediate_tmp qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        event_index
        ORDER BY
            modified_timestamp DESC
    ) = 1
