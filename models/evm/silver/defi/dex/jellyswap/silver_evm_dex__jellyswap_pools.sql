{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'pool_address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH created_pools AS (
    SELECT 

        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics[1]::STRING as pool_id,
        SUBSTR(topics[1]::STRING, 1, 42) as pool_address,
        _inserted_timestamp,
        _log_id
    FROM 
        {{ ref('silver_evm__logs') }}
    WHERE 
        topics[0]::STRING = '0x3c13bc30b8e878c53fd2a36b679409c073afd75950be43d8858768e956fbc20e' -- PoolRegistered
        AND contract_address = '0xfb43069f6d0473b85686a85f4ce4fc1fd8f00875' -- Vault contract
        AND tx_status = 'SUCCESS'
    
    {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) - INTERVAL '5 minutes'
            FROM {{ this }}
        )
    {% endif %}
)
SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,

    pool_id,
    pool_address,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS jellyswap_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM created_pools
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pool_address 
    ORDER BY block_timestamp DESC
) = 1