{{ config(
    materialized = 'view'
) }}

SELECT
    LOWER(
        A.token_address
    ) AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    is_verified,
    is_verified_modified_timestamp,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id,
    _invocation_id
FROM
    {{ ref(
        'bronze__complete_token_asset_metadata'
    ) }} A
