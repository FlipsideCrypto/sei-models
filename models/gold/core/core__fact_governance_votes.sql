{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}}
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    voter,
    proposal_id,
    vote_option,
    vote_weight,
    memo
FROM
    {{ ref('silver__governance_votes') }}
