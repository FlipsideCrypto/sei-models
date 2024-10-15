{{ config(
  materialized = 'incremental',
  unique_key = "sei_address",
  incremental_strategy = 'merge',
  merge_exclude_columns = ["block_timestamp_associated","block_id_associated","inserted_timestamp"],
  cluster_by = ['block_timestamp_associated::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(sei_address, evm_address);",
  tags = ['core','full_test']
) }}

WITH base AS (

  SELECT
    block_timestamp,
    block_id,
    tx_id,
    msg_index,
    OBJECT_AGG(
      attribute_key :: STRING,
      attribute_value :: variant
    ) AS j,
    j :sei_addr :: STRING AS sei_address,
    j :evm_addr :: STRING AS evm_address
  FROM
    {{ ref('silver__msg_attributes') }} A
  WHERE
    tx_succeeded
    AND msg_type = 'address_associated'

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
GROUP BY
  block_timestamp,
  block_id,
  tx_id,
  msg_index
)
SELECT
  block_timestamp AS block_timestamp_associated,
  block_id AS block_id_associated,
  sei_address,
  LOWER(evm_address) AS evm_address,
  {{ dbt_utils.generate_surrogate_key(
    ['sei_address']
  ) }} AS dim_address_mapping_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  base qualify(ROW_NUMBER() over(PARTITION BY sei_address
ORDER BY
  block_timestamp) = 1)
