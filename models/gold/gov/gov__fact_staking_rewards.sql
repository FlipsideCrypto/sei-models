{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    tags = ['noncore']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    action,
    delegator_address,
    rewards_recipient,
    validator_address,
    amount,
    currency,
    COALESCE (
        staking_rewards_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_unique_id']
        ) }}
    ) AS fact_staking_rewards_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__staking_rewards') }}
