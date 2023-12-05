{{ config(
    materialized = 'incremental',
    unique_key = "_inserted_timestamp",
    incremental_strategy = 'delete+insert',
    tags = ['noncore']
) }}

SELECT
    b.value :coingecko :: STRING AS coingecko,
    b.value :coinmarketcap :: STRING AS coinmarketcap,
    b.value :description :: STRING AS description,
    b.value :id :: STRING AS contract_address,
    b.value :name :: STRING AS NAME,
    b.value :precision :: INT AS decimals,
    b.value :price :: FLOAT AS price,
    b.value :slugs AS slugs,
    b.value :symbol :: STRING AS symbol,
    b.value :type :: STRING AS TYPE,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS prices_api_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze_api__get_prices_api'
    ) }},
    LATERAL FLATTEN(DATA :data) b

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

qualify(ROW_NUMBER() over (PARTITION BY contract_address
ORDER BY
    _inserted_timestamp DESC) = 1)
