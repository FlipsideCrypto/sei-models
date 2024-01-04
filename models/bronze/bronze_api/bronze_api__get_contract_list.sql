{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH ids AS (

    SELECT
        0 id {# UNION ALL
    SELECT
        101UNION ALL
    SELECT
        201
    UNION ALL
    SELECT
        301
    UNION ALL
    SELECT
        401
    UNION ALL
    SELECT
        501
    UNION ALL
    SELECT
        601
    UNION ALL
    SELECT
        701
    UNION ALL
    SELECT
        801 #}
),
params AS (
    SELECT
        'query getContractListQuery { contracts(limit: 100, offset: ' || id || ', order_by: {id: desc}) { address label admin: account { address } init_by: contract_histories(order_by: {block: {timestamp: asc}}, limit: 1) { block { timestamp } account { address } } } }' AS query
    FROM
        ids
),
res AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            'https://pacific-1-graphql.alleslabs.dev/v1/graphql',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'query',
                query,
                'variables',{}
            )
        ) AS res,
        query,
        SYSDATE() AS _inserted_timestamp
    FROM
        params
)
SELECT
    res,
    query,
    C.value :address :: STRING AS contract_address,
    C.value :admin :: STRING AS admin,
    C.value :init_by [0] :account :address :: STRING AS init_by_account_address,
    C.value :init_by [0] :block :timestamp :: datetime AS init_by_block_timestamp,
    C.value :label :: STRING AS label,
    _inserted_timestamp
FROM
    res,
    LATERAL FLATTEN(
        input => res.res :data :data :contracts
    ) C
