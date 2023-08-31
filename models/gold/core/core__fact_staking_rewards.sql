{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_staking_rewards') }}
