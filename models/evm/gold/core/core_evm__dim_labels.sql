{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

select 
    l.blockchain as blockchain,
    l.creator as creator,
    evm_address as address,
    l.label_type as label_type,
    l.label_subtype as label_subtype,
    l.label as label,
    l.address_name as address_name,
    l.dim_labels_id as dim_labels_id,
    l.inserted_timestamp as inserted_timestamp,
    l.modified_timestamp as modified_timestamp
from {{ ref('core__dim_labels') }} l
join {{ ref('core__dim_address_mapping') }} d 
on l.address = d.sei_address