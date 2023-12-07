{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
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
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS dex_metadata_seaswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
