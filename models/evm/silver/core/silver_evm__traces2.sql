-- depends_on: {{ ref('bronze_evm__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    incremental_predicates = [fsc_evm.standard_predicate()],
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    full_refresh = false,
    tags = ['core'],
    enabled = false
) }}
{{ fsc_evm.silver_traces_v1(
    full_reload_start_block = 80000000,
    full_reload_blocks = 5000000,
    use_partition_key = true,
    schema_name = 'bronze_evm',
    sei_traces_mode = true
) }}
