{{ config (
  materialized = 'view'
) }}

SELECT
  block_number AS block_id,
  COALESCE(
    b.value,
    C.value
  ) :hash :: STRING AS tx_id,
  COALESCE(
    b.index,
    C.index
  ) AS tx_block_index,
  COALESCE(
    b.value,
    C.value
  ) AS tx,
  _inserted_timestamp
FROM
  {{ source(
    'lq',
    'lq_txs'
  ) }} A,
  LATERAL FLATTEN(
    input => A.data :result :txs,
    outer => TRUE
  ) AS b,
  LATERAL FLATTEN(
    input => A.data :data :result :txs,
    outer => TRUE
  ) AS C
WHERE
  DATA :error :code IS NULL {# UNION ALL
SELECT
  block_number AS block_id,
  COALESCE(
    b.value,
    C.value
  ) :hash :: STRING AS tx_id,
  COALESCE(
    b.index,
    C.index
  ) AS tx_block_index,
  COALESCE(
    b.value,
    C.value
  ) AS tx,
  _inserted_timestamp
FROM
  {{ source(
    'lq',
    'lq_txs_101'
  ) }} A,
  LATERAL FLATTEN(
    input => A.data :result :txs,
    outer => TRUE
  ) AS b,
  LATERAL FLATTEN(
    input => A.data :data :result :txs,
    outer => TRUE
  ) AS C
WHERE
  DATA :error :code IS NULL #}
