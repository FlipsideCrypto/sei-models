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
        'address_labels'
    ) }}
WHERE
    blockchain = 'sei'

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
