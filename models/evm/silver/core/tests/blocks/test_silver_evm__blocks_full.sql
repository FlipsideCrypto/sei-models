{{ config (
    materialized = 'view',
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__blocks') }}
WHERE
    block_timestamp < DATEADD('hour', -1, SYSDATE())
