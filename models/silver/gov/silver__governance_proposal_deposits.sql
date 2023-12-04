{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

proposal_ids AS (
    SELECT
        tx_id,
        attribute_value AS proposal_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'proposal_id'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
deposit_value AS (
    SELECT
        tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        {{ ref('silver__msg_attributes') }}
        m
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'amount'
        AND attribute_value IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
depositors AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS depositor
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    msg_index)) = 1
)
SELECT
    block_id,
    block_timestamp,
    p.tx_id,
    tx_succeeded,
    d.depositor,
    p.proposal_id :: NUMBER AS proposal_id,
    v.amount :: FLOAT AS amount,
    v.currency,
    {{ dbt_utils.generate_surrogate_key(
        ['v.tx_id']
    ) }} AS governance_proposal_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    deposit_value v
    INNER JOIN proposal_ids p
    ON p.tx_id = v.tx_id
    INNER JOIN depositors d
    ON v.tx_id = d.tx_id
    LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON v.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
