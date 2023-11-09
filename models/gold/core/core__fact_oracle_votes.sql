{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICE' }} },
    tags = ['daily']
) }}

SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.msg_group,
    A.msg_sub_group,
    A.tx_sender,
    A.voter,
    A.amount,
    A.currency
FROM
    {{ ref('silver__oracle_votes') }} A
