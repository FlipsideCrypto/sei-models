{{ config (
  materialized = 'view'
) }}

SELECT
  block_number AS block_id,
  COALESCE(ARRAY_SIZE(COALESCE(b.value, C.value) :data :txs) :: NUMBER, 0) AS tx_count,
  COALESCE(
    b.value,
    C.value
  ) :header AS header,
  header :chain_id :: STRING AS chain_id,
  header :time :: datetime AS block_timestamp,
  A.data,
  _inserted_timestamp
FROM
  {{ source(
    'lq',
    'lq_blocks'
  ) }} A,
  LATERAL FLATTEN(
    input => A.data :result,
    outer => TRUE
  ) AS b,
  LATERAL FLATTEN(
    input => A.data :data :result,
    outer => TRUE
  ) AS C
WHERE
  key = 'block'
