{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sailorswap_pools_id',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH pools_created AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '[a-f0-9]{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics[1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics[2] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data[1] :: STRING, 25, 40)) AS pool_address,
    FROM
        sei.silver_evm.logs
    WHERE
        contract_address = LOWER('0xA51136931fdd3875902618bF6B3abe38Ab2D703b') -- SailorSwapFactory
        AND topics[0] :: STRING = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' -- PairCreated

{% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '5 minutes'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    token0,
    token1,
    pool_address,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS sailorswap_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pools_created qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    block_timestamp DESC)) = 1

