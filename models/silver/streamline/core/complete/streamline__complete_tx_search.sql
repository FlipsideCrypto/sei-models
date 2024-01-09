-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    block_number,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
WHERE
    DATA <> '[]'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
