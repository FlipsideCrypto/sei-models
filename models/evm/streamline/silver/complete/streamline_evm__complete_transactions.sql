-- depends_on: {{ ref('bronze_evm__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_core_complete']
) }}

SELECT
    id,
    block_number,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze_evm__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_evm__streamline_FR_transactions') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
