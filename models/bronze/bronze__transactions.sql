{{ config (
  materialized = 'view'
) }}

SELECT
  block_number AS block_id,
  b.value :hash :: STRING AS tx_id,
  INDEX AS tx_block_index,
  b.value AS tx,
  _inserted_timestamp
FROM
  {{ source(
    'lq',
    'lq_txs'
  ) }} A,
  LATERAL FLATTEN(
    input => A.data :result :txs,
    outer => TRUE
  ) AS b
WHERE
  DATA :error :code IS NULL
