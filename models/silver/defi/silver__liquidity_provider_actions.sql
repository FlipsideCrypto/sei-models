WITH msg_atts AS (
    SELECT
        *
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
rollups AS (
    SELECT
        A.tx_id,
        A.msg_index,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :action :: STRING AS action,
        j :_contract_address :: STRING AS pool_address,
        COALESCE(
            j :assets,
            j :refund_assets
        ) :: STRING AS assets,
        j :sender :: STRING AS lper,
        COALESCE(
            j :share,
            j :withdrawn_share
        ) :: INT AS lp_shares
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
                AND attribute_key = 'action'
                AND attribute_value IN (
                    'provide_liquidity',
                    'withdraw_liquidity'
                )
        ) b
        ON A.block_timestamp :: DATE = b.bd
        AND A.tx_id = b.tx_id
        AND A.msg_index = b.msg_index
    GROUP BY
        A.tx_id,
        A.msg_index
),
splits AS (
    SELECT
        tx_id,
        msg_index,
        action,
        pool_address,
        TRIM(
            b.value
        ) AS asset_raw,
        SPLIT_PART(
            REGEXP_REPLACE(
                asset_raw,
                '[^[:digit:]]',
                ' '
            ),
            ' ',
            0
        ) AS amount,
        RIGHT(asset_raw, LENGTH(asset_raw) - LENGTH(SPLIT_PART(REGEXP_REPLACE(asset_raw, '[^[:digit:]]', ' '), ' ', 0))) AS currency,
        lper,
        lp_shares
    FROM
        rollups A,
        LATERAL SPLIT_TO_TABLE(
            assets,
            ','
        ) AS b
)
SELECT
    b.tx_id,
    b.block_timestamp,
    b._inserted_timestamp,
    A.msg_index,
    A.action,
    A.pool_address,
    A.asset_raw,
    A.amount,
    A.currency,
    A.lper,
    A.lp_shares
FROM
    splits A
    JOIN (
        SELECT
            DISTINCT tx_id,
            block_timestamp,
            _inserted_timestamp
        FROM
            msg_atts
    ) b
    ON A.tx_id = b.tx_id
