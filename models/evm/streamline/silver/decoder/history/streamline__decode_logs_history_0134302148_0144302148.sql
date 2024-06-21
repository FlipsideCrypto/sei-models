{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2( 
        func = 'streamline.udf_bulk_decode_logs_v2', 
        target = "{{this.schema}}.{{this.identifier}}", 
        params ={ "external_table" :"decoded_logs", 
        "sql_limit" :"7500000", 
        "producer_batch_size" :"400000", 
        "worker_batch_size" :"200000", 
        "sql_source" :"{{this.identifier}}" } ), 
        if_data_call_wait() 
    ],
    tags = ['streamline_decoded_logs_history']
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ decode_logs_history(
    start,
    stop
) }}
