{{ config (
    materialized = 'view'
) }}

select * from {{ ref('bronze_evm__streamline_fr_blocks_v1') }}
union all
select * from {{ ref('bronze_evm__streamline_fr_blocks_v2') }}