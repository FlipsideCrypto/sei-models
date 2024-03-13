{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore','recent_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    depositor,
    proposal_id,
    amount,
    currency,
    COALESCE (
        governance_proposal_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_governance_proposal_deposits_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__governance_proposal_deposits') }}
