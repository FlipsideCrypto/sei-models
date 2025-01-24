{{ config (
    materialized = 'view',
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('core_evm__fact_traces') }}
WHERE
    block_timestamp < DATEADD('hour', -1, SYSDATE())
