{{ config(
    materialized = 'incremental',
    unique_key = ["token_address","token_chain"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    enabled = false
) }}

WITH msg_atts AS (

    SELECT
        block_timestamp,
        tx_id,
        msg_type,
        msg_index,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'wasm'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
prefinal AS (
    SELECT
        A.block_timestamp,
        A.tx_id,
        A.msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :_contract_address :: STRING AS _contract_address,
        j :action :: STRING AS action,
        j :token_address :: STRING AS token_address,
        j :token_chain :: INT AS token_chain,
        _inserted_timestamp
    FROM
        msg_atts A
        JOIN (
            SELECT
                tx_id,
                block_timestamp :: DATE bd,
                msg_index
            FROM
                msg_atts
            WHERE
                msg_type = 'wasm'
                AND attribute_key = '_contract_address'
                AND attribute_value = 'sei1smzlm9t79kur392nu9egl8p8je9j92q4gzguewj56a05kyxxra0qy0nuf3'
        ) b
        ON A.block_timestamp :: DATE = b.bd
        AND A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    GROUP BY
        A.block_timestamp,
        A.tx_id,
        A.msg_index,
        _inserted_timestamp
    HAVING(
            action = 'register_asset'
        )
)
SELECT
    block_timestamp,
    tx_id,
    msg_index,
    _contract_address AS contract_address,
    action,
    token_address,
    token_chain,
    _inserted_timestamp
FROM
    prefinal qualify(ROW_NUMBER() over (PARTITION BY token_address, token_chain
ORDER BY
    block_timestamp DESC) = 1)
