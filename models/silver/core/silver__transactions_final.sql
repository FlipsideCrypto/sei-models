{{ config(
    materialized = 'view',
    tags = ['v2']
) }}

WITH atts AS (

    SELECT
        tx_id,
        msg_index,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'tx'
        AND attribute_key IN (
            'fee',
            'acc_seq'
        )
),
fee AS (
    SELECT
        tx_id,
        attribute_value AS fee
    FROM
        atts
    WHERE
        attribute_key = 'fee' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
spender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_from
    FROM
        atts
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    s.tx_from,
    tx_succeeded,
    codespace,
    COALESCE(
        fee,
        '0usei'
    ) AS fee,
    gas_used,
    gas_wanted,
    tx_code,
    msgs,
    {{ dbt_utils.generate_surrogate_key(
        ['t.tx_id']
    ) }} AS transactions_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    t._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }}
    t
    LEFT OUTER JOIN fee f
    ON t.tx_id = f.tx_id
    LEFT OUTER JOIN spender s
    ON t.tx_id = s.tx_id
