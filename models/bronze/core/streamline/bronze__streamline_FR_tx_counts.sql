{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    DATA,
    _INSERTED_TIMESTAMP,
    id,
    metadata,
    _PARTITION_BY_BLOCK_ID,
    VALUE
FROM
    {{ ref('bronze__streamline_FR_tx_counts_v2') }}
UNION ALL
SELECT
    block_number,
    DATA,
    _INSERTED_TIMESTAMP,
    id,
    metadata,
    _PARTITION_BY_BLOCK_ID,
    VALUE
FROM
    {{ ref('bronze__streamline_FR_tx_counts_v1') }}
