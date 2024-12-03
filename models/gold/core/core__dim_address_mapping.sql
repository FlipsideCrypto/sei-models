{{ config(
  materialized = 'incremental',
  unique_key = "sei_address",
  incremental_strategy = 'merge',
  merge_exclude_columns = ["block_timestamp_associated","block_id_associated","inserted_timestamp"],
  cluster_by = ['block_timestamp_associated::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(sei_address, evm_address);",
  tags = ['core','full_test']
) }}

WITH onchain AS (

  SELECT
    block_timestamp_associated,
    block_id_associated,
    sei_address,
    evm_address
  FROM
    {{ ref('silver__address_mapping_onchain') }} A

{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(
        modified_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}
),
api AS (
  SELECT
    block_timestamp_associated,
    block_id_associated,
    sei_address,
    evm_address
  FROM
    {{ ref('silver__address_mapping_api') }} A
  WHERE
    sei_address NOT IN (
      SELECT
        sei_address
      FROM
        {{ ref('silver__address_mapping_onchain') }}
    )

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(
      modified_timestamp
    )
  FROM
    {{ this }}
)
{% endif %}
),
combo AS (
  SELECT
    block_timestamp_associated,
    block_id_associated,
    sei_address,
    evm_address
  FROM
    onchain
  UNION ALL
  SELECT
    block_timestamp_associated,
    block_id_associated,
    sei_address,
    evm_address
  FROM
    api
)
SELECT
  block_timestamp_associated,
  block_id_associated,
  sei_address,
  LOWER(evm_address) AS evm_address,
  {{ dbt_utils.generate_surrogate_key(
    ['sei_address']
  ) }} AS dim_address_mapping_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combo
