-- depends_on: {{ ref('bronze__streamline_sei_addresses') }}
{{ config (
    materialized = "incremental",
    unique_key = "evm_address"
) }}

SELECT
    VALUE :EVM_ADDRESS :: STRING AS evm_address,
    {{ dbt_utils.generate_surrogate_key(
        ['EVM_ADDRESS']
    ) }} AS complete_sei_addresses_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_sei_addresses') }}
{% else %}
    {{ ref('bronze__streamline_FR_sei_addresses') }}
{% endif %}
WHERE
    1 = 1 --DATA :result LIKE 'sei%'

{% if is_incremental() %}
AND _inserted_timestamp >=(
    SELECT
        COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
    FROM
        {{ this }})
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY evm_address
    ORDER BY
        _inserted_timestamp DESC)) = 1
