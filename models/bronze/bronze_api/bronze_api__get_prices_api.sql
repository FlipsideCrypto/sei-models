{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'get',
        'https://celatone-api.alleslabs.dev/assets/sei/pacific-1/prices',{},{}
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
