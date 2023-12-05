{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
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
        AND attribute_value = '/cosmos.gov.v1beta1.MsgVote'
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
        JOIN tx_mg_msg b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
        AND A.block_timestamp = b.block_timestamp
        LEFT JOIN LATERAL FLATTEN (
            input => TRY_PARSE_JSON(attribute_value),
            outer => TRUE
        ) C
    WHERE
        msg_type || attribute_key IN (
            'proposal_voteproposal_id',
            'proposal_voteoption',
            'messagesender'
        )
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
        j :proposal_id :: INT AS proposal_id,
        j :sender :: STRING AS voter,
        j :option :: STRING AS vote_option,
        j :weight :: FLOAT AS vote_weight
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
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    proposal_id,
    voter,
    vote_option,
    vote_weight,
    NULL AS memo,
    concat_ws(
        '-',
        tx_id,
        msg_group,
        msg_sub_group,
        voter
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS governance_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    agg
