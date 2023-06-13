{{ config (
  materialized = 'view'
) }}

SELECT
  block_number AS block_id,
  DATA :hash :: STRING AS tx_id,
  DATA :index AS tx_block_index,
  DATA AS tx,
  NULL AS _inserted_timestamp
FROM
  {{ source(
    'streamline',
    'tx_search',
  ) }}
WHERE
  DATA :error :code IS NULL
