{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
) }}

SELECT
    *
FROM
    {{ ref('gov__fact_governance_submit_proposal') }}
