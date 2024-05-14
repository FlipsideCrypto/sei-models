{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH base AS (

    SELECT
        *
    FROM
        (
            SELECT
                *,
                conditional_true_event(
                    CASE
                        WHEN rn_mod_out = 1 THEN TRUE
                        ELSE FALSE
                    END
                ) over (
                    ORDER BY
                        min_block
                ) groupID_out
            FROM
                (
                    SELECT
                        *,
                        MOD(ROW_NUMBER() over(
                    ORDER BY
                        min_block), 100) rn_mod_out
                    FROM
                        (
                            SELECT
                                MIN(
                                    block_id :: INT
                                ) min_block,
                                MAX(
                                    block_id :: INT
                                ) max_block,
                                ARRAY_AGG(block_id) blocks
                            FROM
                                (
                                    SELECT
                                        conditional_true_event(
                                            CASE
                                                WHEN rn_mod = 1 THEN TRUE
                                                ELSE FALSE
                                            END
                                        ) over (
                                            ORDER BY
                                                block_ID :: INT
                                        ) groupID,
                                        block_id
                                    FROM
                                        (
                                            SELECT
                                                block_Id :: STRING block_Id,
                                                MOD(ROW_NUMBER() over(
                                            ORDER BY
                                                block_id :: INT), 20) rn_mod
                                            FROM
                                                (
                                                    SELECT
                                                        DISTINCT block_id
                                                    FROM
                                                        {{ ref('silver__blocks') }}

{% if is_incremental() %}
EXCEPT
SELECT
    block_id
FROM
    silver.blockchain
{% endif %}
)
)
)
GROUP BY
    groupID
)
)
)
WHERE
    groupID_out < 20
),
calls AS (
    SELECT
        groupid_out,
        ARRAY_AGG(
            { 'jsonrpc': '2.0',
            'id': min_block :: INT,
            'method': 'blockchain',
            'params': [min_block::STRING,max_block::STRING] }
        ) call
    FROM
        base
    GROUP BY
        groupid_out
)
SELECT
    call,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        call,
        'Vault/prod/sei/quicknode/mainnet'
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
FROM
    calls
