{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_id,
    block_timestamp,
    chain_id,
    tx_count,
    proposer_address,
    validator_hash,
    header,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id']
        ) }}
    ) AS fact_blocks_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__blocks') }}
