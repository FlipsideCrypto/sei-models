{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'merge'
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ source(
        'crosschain',
        'dim_address_labels'
    ) }}
WHERE
    blockchain = 'sei'

{% if is_incremental() %}
AND insert_date >= (
    SELECT
        MAX(
            insert_date
        )
    FROM
        {{ this }}
)
{% endif %}
