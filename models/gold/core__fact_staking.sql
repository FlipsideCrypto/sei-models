{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}}
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_caller_address,
    action,
    delegator_address,
    validator_address,
    redelegate_source_validator_address,
    amount,
    currency,
    completion_time
FROM
    {{ ref('silver__staking') }}
