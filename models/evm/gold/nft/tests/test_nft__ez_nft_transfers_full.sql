{{ config (
    materialized = "view",
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('nft__ez_nft_transfers') }}
