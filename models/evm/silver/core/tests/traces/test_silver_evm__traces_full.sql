{{ config (
    materialized = 'view',
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__traces') }}
WHERE
    block_timestamp < DATEADD('hour', -1, SYSDATE())
