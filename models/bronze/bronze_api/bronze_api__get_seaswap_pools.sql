{{ config(
    materialized = 'incremental',
    full_refresh = false,
    enabled = false
) }}

WITH perms AS (

    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) :: STRING AS id
    FROM
        TABLE(GENERATOR(rowcount => 10))
),
base AS(
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://db.seaswap.io/v1/pools?page=' || id,{},{}
        ) AS DATA,
        SYSDATE() AS _inserted_timestamp
    FROM
        perms
)
SELECT
    b.value :__v :: STRING AS version,
    b.value :_id :: STRING AS pool_id,
    b.value :lp_token_address :: STRING AS lp_token_address,
    b.value :pool_address :: STRING AS pool_address,
    b.value :pool_name :: STRING AS pool_name,
    b.value :pool_type :: STRING AS pool_type,
    b.value :token1 :_id :: STRING AS token1_id,
    b.value :token1 :denom :: STRING AS token1_denom,
    b.value :token1 :name :: STRING AS token1_name,
    b.value :token1 :supply :: INT AS token1_supply,
    b.value :token1 :symbol :: STRING AS token1_symbol,
    b.value :token1 :type :: STRING AS token1_type,
    b.value :token2 :_id :: STRING AS token2_id,
    b.value :token2 :denom :: STRING AS token2_denom,
    b.value :token2 :name :: STRING AS token2_name,
    b.value :token2 :supply :: INT AS token2_supply,
    b.value :token2 :symbol :: STRING AS token2_symbol,
    b.value :token2 :type :: STRING AS token2_type,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN(
        DATA :data
    ) b
