{{ config(
    materialized = 'incremental',
    unique_key = 'pool_id',
    incremental_strategy = 'merge',
    enabled = true,
    tags = ['noncore']
) }}

SELECT
    version,
    pool_id,
    lp_token_address,
    pool_address,
    pool_name,
    pool_type,
    token1_id,
    token1_denom,
    token1_name,
    token1_supply,
    token1_symbol,
    token1_type,
    token2_id,
    token2_denom,
    token2_name,
    token2_supply,
    token2_symbol,
    token2_type,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__get_seaswap_pools') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY pool_id
ORDER BY
    _inserted_timestamp DESC) = 1)
