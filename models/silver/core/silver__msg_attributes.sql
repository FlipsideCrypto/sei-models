{{ config(
  materialized = 'view',
  tags = ['v2']
) }}

SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  tx_type,
  msg_group,
  msg_sub_group,
  msg_index,
  msg_type,
  b.index AS attribute_index,
  TRY_BASE64_DECODE_STRING(
    b.value :key :: STRING
  ) AS attribute_key,
  TRY_BASE64_DECODE_STRING(
    b.value :value :: STRING
  ) AS attribute_value,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index','attribute_index']
  ) }} AS msg_attributes_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('silver__msgs') }} A,
  LATERAL FLATTEN(
    input => A.msg,
    path => 'attributes'
  ) b
