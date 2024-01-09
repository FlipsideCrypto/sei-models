{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH WORK AS (

    SELECT
        top 10 contract_address
    FROM
        {{ target.database }}.silver.contracts
    WHERE
        label IS NULL
),
res AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'GET',
            'https://celatone-api.alleslabs.dev/v1/sei/pacific-1/contracts/' || contract_address || '/info?is_gov=false',{},{}
        ) AS res,
        SYSDATE() AS _inserted_timestamp
    FROM
        WORK
)
SELECT
    res,
    res :data :contract_rest :address :: STRING AS contract_address,
    res :data :contract_rest :contract_info :admin :: STRING AS admin,
    res :data :contract_rest :contract_info: creator :: STRING AS init_by_account_address,
    res :data :contract_rest :contract_info: label :: STRING AS label,
    _inserted_timestamp
FROM
    res
