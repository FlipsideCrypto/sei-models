{{ config (
    materialized = "view",
    tags = ['silver','scores','phase_4']
) }}

select * from {{ source('data_science_silver', 'scoring_activity_categories') }}