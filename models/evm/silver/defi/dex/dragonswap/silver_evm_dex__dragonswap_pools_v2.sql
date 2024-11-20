{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'pool_address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['curated']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS pool_id,
    FROM
        {{ ref ('silver_evm__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND contract_address = LOWER('0x179d9a5592bc77050796f7be28058c51ca575df4') --DragonswapFactoryV2
        AND topics [0] :: STRING = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'

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
    pool_id,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS dragonswap_pools_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    created_pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    block_timestamp DESC)) = 1
