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
    proposer,
    proposal_id,
    proposal_type,
    proposal_title,
    proposal_description,
    COALESCE (
        governance_submit_proposal_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_governance_submit_proposal_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__governance_submit_proposal') }}
