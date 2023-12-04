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
        block_id,
        block_timestamp,
        tx_succeeded,
        _inserted_timestamp,
        attribute_value AS proposal_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'submit_proposal'
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
proposal_type AS (
    SELECT
        tx_id,
        attribute_value AS proposal_type
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'submit_proposal'
        AND attribute_key = 'proposal_type'

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
proposer AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS proposer
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
        max_date
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
    proposer,
    p.proposal_id :: NUMBER AS proposal_id,
    y.proposal_type,
    NULL AS proposal_title,
    NULL AS proposal_description,
    {# COALESCE(
    tx_body :messages [0] :content :title,
    tx_body :messages [0] :msgs [0] :content :title
) :: STRING AS proposal_title,
COALESCE(
    tx_body :messages [0] :content :description,
    tx_body :messages [0] :msgs [0] :content :description
) :: STRING AS proposal_description,
#}
{{ dbt_utils.generate_surrogate_key(
    ['p.tx_id']
) }} AS governance_submit_proposal_id,
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp,
_inserted_timestamp,
'{{ invocation_id }}' AS _invocation_id
FROM
    proposal_ids p
    INNER JOIN proposal_type y
    ON p.tx_id = y.tx_id
    INNER JOIN proposer pp
    ON p.tx_id = pp.tx_id {# LEFT OUTER JOIN {{ ref('silver__transactions') }}
    t
    ON p.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}

#}
