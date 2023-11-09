{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore']
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
    proposal_description
FROM
    {{ ref('silver__governance_submit_proposal') }}
