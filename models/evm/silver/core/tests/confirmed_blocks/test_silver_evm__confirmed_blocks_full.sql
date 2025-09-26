{{ config (
    materialized = 'view',
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__confirmed_blocks') }}
WHERE
    block_number not in (169970057, 169970054)