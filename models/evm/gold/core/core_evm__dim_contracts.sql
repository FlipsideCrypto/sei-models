{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    c0.created_contract_address AS address,
    c1.token_symbol AS symbol,
    c1.token_name AS NAME,
    c1.token_decimals AS decimals,
    c0.block_number AS created_block_number,
    c0.block_timestamp AS created_block_timestamp,
    c0.tx_hash AS created_tx_hash,
    c0.creator_address AS creator_address,
    c0.created_contracts_id AS dim_contracts_id,
    GREATEST(
        c0.inserted_timestamp,
        c1.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        c0.modified_timestamp,
        c1.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_evm__created_contracts') }}
    c0
    LEFT JOIN {{ ref('silver_evm__contracts') }}
    c1
    ON LOWER(
        c0.created_contract_address
    ) = LOWER(
        c1.contract_address
    )
