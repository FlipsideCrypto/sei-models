{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    enabled = true,
    tags = ['noncore']
) }}

WITH

{% if is_incremental() %}
md AS (

    SELECT
        MAX(_inserted_timestamp) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

ini AS (
    SELECT
        init_tx_id AS tx_id,
        init_by_block_timestamp :: DATE bd
    FROM
        {{ ref('silver__contracts_instantiate') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            md
    )
{% endif %}
INTERSECT
SELECT
    tx_id,
    block_timestamp :: DATE bd
FROM
    {{ ref('silver__msg_attributes') }}
WHERE
    msg_type = 'execute'
    AND attribute_key = '_contract_address'
    AND attribute_value = 'sei1xr3rq8yvd7qplsw5yx90ftsr2zdhg4e9z60h5duusgxpv72hud3shh3qfl'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        md
)
{% endif %}
),
msg_atts AS (
    SELECT
        A.tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value,
        MAX(
            CASE
                WHEN attribute_key = 'action' THEN attribute_value
            END
        ) over(
            PARTITION BY A.tx_id,
            A.msg_index
        ) AS action,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }} A
        JOIN ini b
        ON A.tx_id = b.tx_id
        AND A.block_timestamp :: DATE = b.bd
    WHERE
        msg_type = 'wasm'
        AND attribute_key IN (
            'liquidity_token_addr',
            'pair_contract_addr',
            'action'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        md
)
{% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    attribute_value AS contract_address,
    attribute_key AS TYPE,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS dex_metadata_astroport_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    msg_atts A
WHERE
    COALESCE(
        action,
        ''
    ) NOT IN (
        'deregister',
        'deactivate_pool'
    )
    AND attribute_key IN (
        'liquidity_token_addr',
        'pair_contract_addr'
    )
