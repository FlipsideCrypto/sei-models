{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    incremental_predicates = [fsc_evm.standard_predicate()],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core'],
    enabled = false
) }}
{{ fsc_evm.gold_traces_v1(
    full_reload_start_block = 110000000,
    full_reload_blocks = 5000000,
    schema_name = 'silver_evm',
    sei_traces_mode = true
) }}
