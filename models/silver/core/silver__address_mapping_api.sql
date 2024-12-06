{{ config(
  materialized = 'incremental',
  unique_key = "sei_address",
  incremental_strategy = 'merge',
  tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_sei_addresses') }}
WITH base AS (

  SELECT
    VALUE :EVM_ADDRESS :: STRING AS evm_address,
    DATA :result :: STRING AS sei_address,
    _inserted_timestamp
  FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_sei_addresses') }}
WHERE
  _inserted_timestamp >= DATEADD(
    MINUTE,
    -15,(
      SELECT
        MAX(
          _inserted_timestamp
        )
      FROM
        {{ this }}
    )
  )
{% else %}
  {{ ref('bronze__streamline_FR_sei_addresses') }}
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY evm_address
ORDER BY
  _inserted_timestamp DESC)) = 1
)
SELECT
  b.block_timestamp_associated,
  b.block_id_associated,
  A.sei_address,
  LOWER(
    A.evm_address
  ) AS evm_address,
  {{ dbt_utils.generate_surrogate_key(
    ['sei_address']
  ) }} AS address_mapping_api_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  base A
  JOIN {{ ref('streamline__evm_addresses') }}
  b
  ON A.evm_address = b.evm_address
