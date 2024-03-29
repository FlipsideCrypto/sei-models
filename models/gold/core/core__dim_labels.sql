{{ config(
    materialized = 'view',
    tags = ['core','recent_test']
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name AS address_name,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__croschain_labels') }}
