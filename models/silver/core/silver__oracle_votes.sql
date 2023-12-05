{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['daily']
) }}

WITH base_ma AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}

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
),
tx_mg_msg AS (
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        msg_sub_group
    FROM
        base_ma
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value = '/seiprotocol.seichain.oracle.MsgAggregateExchangeRateVote'
),
vote_msgs AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        A.msg_group,
        A.msg_sub_group,
        COALESCE(
            C.key,
            A.attribute_key
        ) AS attribute_key,
        COALESCE(
            C.value,
            A.attribute_value
        ) AS attribute_value,
        _inserted_timestamp
    FROM
        base_ma A
        LEFT JOIN tx_mg_msg b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
        AND A.block_timestamp = b.block_timestamp
        LEFT JOIN LATERAL FLATTEN (
            input => TRY_PARSE_JSON(attribute_value),
            outer => TRUE
        ) C
    WHERE
        (
            b.tx_id IS NOT NULL
            OR (
                A.msg_type = 'tx'
                AND A.attribute_key = 'acc_seq'
            )
        )
        AND msg_type = 'aggregate_vote'
),
agg AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :voter :: STRING AS voter,
        j :exchange_rates :: STRING AS exchange_rates
    FROM
        vote_msgs
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        _inserted_timestamp
),
votes_split AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        b.value AS split_rates
    FROM
        agg,
        LATERAL SPLIT_TO_TABLE(
            exchange_rates,
            ','
        ) b
),
tx_sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_sender
    FROM
        base_ma
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_group,
    A.msg_sub_group,
    b.tx_sender,
    A.voter,
    SPLIT_PART(
        TRIM(
            REGEXP_REPLACE(
                split_rates,
                '[^[:digit:].]',
                ' '
            )
        ),
        ' ',
        0
    ) :: DECIMAL(
        38,
        18
    ) AS amount,
    RIGHT(split_rates, LENGTH(split_rates) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(split_rates, '[^[:digit:].]', ' ')), ' ', 0))) :: STRING AS currency,
    split_rates AS raw_exchange_rates,
    concat_ws(
        '-',
        A.tx_id,
        A.msg_group,
        A.msg_sub_group,
        voter,
        currency
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS oracle_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    agg A
    JOIN tx_sender b
    ON A.tx_id = b.tx_id
    JOIN votes_split C
    ON A.tx_id = C.tx_id
    AND A.msg_group = C.msg_group
    AND A.msg_sub_group = C.msg_sub_group
